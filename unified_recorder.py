#!/usr/bin/env python3
"""
Stream Ingestion Engine V5 - Post-Process Downmix + strftime
Key changes:
1. Always copy audio during recording (no real-time downmixing)
2. Use FFmpeg -strftime 1 for automatic timestamping
3. Post-process segments for downmixing if enabled
4. Maintains exact file naming: seg_source_epoch_duration.ext
"""

import subprocess
import time
import json
import boto3
import threading
import shutil
import traceback
import os
from pathlib import Path
from datetime import datetime, timezone
import signal
import sys
import logging
import logging.handlers
import argparse
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

DEFAULT_SEGMENTS_DIR = '/app/segments'

class StreamIngestionEngineV5:
    def __init__(self, config_file='test_config.json'):
        with open(config_file) as f:
            self.config = json.load(f)
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        self.recording_start_time = None

        # Apply environment overrides (keeping Quynh's comprehensive list)
        self.apply_environment_overrides()
        
        # Validate configuration
        self.validate_configuration()

        # AWS clients
        self.s3 = boto3.client('s3', region_name=self.config['aws_region'])

        # File system setup
        if self.config.get('bucket_path') is None:
            self.config['bucket_path'] = f"processing/continuous/{self.config['core_id']}/{self.config['scheduled_job_id']}"
        if self.config.get('local_segments_dir') is None:
            self.config['local_segments_dir'] = DEFAULT_SEGMENTS_DIR

        self.segments_dir = Path(self.config['local_segments_dir'])
        self.segments_dir.mkdir(exist_ok=True, parents=True)

        # Thread pools - separate for processing and uploading
        self.upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="S3Upload")
        self.process_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="PostProcess")

        # Process management
        self.running = True
        self.ffmpeg_process = None

        # Connection retry configuration
        self.max_connection_retries = self.config.get('max_connection_retries', 5)
        self.base_retry_delay = self.config.get('base_retry_delay_seconds', 30)
        self.max_retry_delay = self.config.get('max_retry_delay_seconds', 300)

        # Statistics
        self.stats = {
            'segments_created': 0,
            'segments_uploaded': 0,
            'segments_downmixed': 0,
            'upload_failures': 0,
            'connection_retries': 0,
            'reconnections': 0,
            'stall_recoveries': 0,
            'recording_start_time': None,
            'process_start_time': datetime.now(timezone.utc)
        }

        # Signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

        self.logger.info("=== Stream Ingestion Engine V5 ===")
        self.logger.info("Major improvements:")
        self.logger.info("  1. FFmpeg -strftime for automatic timestamping")
        self.logger.info("  2. Post-process downmixing (sync preserved)")
        self.logger.info("  3. No temp files or complex timing logic")
        
        self.log_processing_mode()
        self.cleanup_old_files()
        self.retry_failed_uploads()

    def apply_environment_overrides(self):
        """Apply Quynh's comprehensive environment variable overrides"""
        logger = logging.getLogger(__name__)
        logger.info("Checking for Kubernetes/Docker environment overrides...")

        # All the environment mappings from Quynh's code
        env_overrides = {
            'MEDIA_SOURCE_ID': ('media_source_id', str),
            'SCHEDULED_JOB_ID': ('scheduled_job_id', str),
            'CORE_ID': ('core_id', str),
            'MEDIA_SOURCE_URL': ('media_source_url', str),
            "URL": ('media_source_url', str),
            'MEDIA_SOURCE_PASS_PHRASE': ('media_source_pass_phrase', str),
            'START_DATE_TIME': ('start_date_time', float),
            'STOP_DATE_TIME': ('stop_date_time', float),
            'SEGMENT_LENGTH_IN_SECONDS': ('segment_length_in_seconds', int),
            'LOCAL_SEGMENTS_DIR': ('local_segments_dir', str),
            'SRT_LATENCY': ('srt_latency', int),
            'SRT_MAXBW': ('srt_maxbw', int),
            'SRT_RCVBUF': ('srt_rcvbuf', int),
            'SRT_SNDBUF': ('srt_sndbuf', int),
            'SRT_PBKEYLEN': ('srt_pbkeylen', int),
            'BUCKET': ('bucket', str),
            'BUCKET_PATH': ('bucket_path', str),
            'AWS_REGION': ('aws_region', str),
            'AUDIO_DOWNMIX': ('audio_downmix', lambda x: x.lower() == 'true'),
            'AUDIO_BITRATE': ('audio_bitrate', str),
            'AUDIO_STREAM_INDEX': ('audio_stream_index', str),
            'VIDEO_STREAM_INDEX': ('video_stream_index', int),
            'OUTPUT_FORMAT': ('output_format', str),
            'MAX_CONNECTION_RETRIES': ('max_connection_retries', int),
            'BASE_RETRY_DELAY_SECONDS': ('base_retry_delay_seconds', int),
            'MAX_RETRY_DELAY_SECONDS': ('max_retry_delay_seconds', int),
            'PERFORMANCE_WARNING_THRESHOLD': ('performance_warning_threshold', float),
            'STARTUP_OFFSET_SECONDS': ('startup_offset_seconds', int),
        }

        overrides_applied = []
        for env_var, (config_key, converter) in env_overrides.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                try:
                    converted_value = converter(env_value)
                    original_value = self.config.get(config_key, 'NOT_SET')
                    self.config[config_key] = converted_value
                    overrides_applied.append(f"{env_var}={env_value} -> {config_key}")
                    logger.info(f"ENV OVERRIDE: {env_var}={env_value} (was: {original_value})")
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid environment variable {env_var}={env_value}: {e}")

        if overrides_applied:
            logger.info(f"Applied {len(overrides_applied)} environment overrides")
        else:
            logger.info("No environment overrides found - using config file values")

    def validate_configuration(self):
        """Validate critical configuration settings"""
        self.logger.info("Validating configuration...")

        # Set defaults FIRST (before validation)
        self.config.setdefault('scheduled_job_id', '0')
        self.config.setdefault('core_id', '0')
        self.config.setdefault('segment_length_in_seconds', 300)

        # Now validate only truly required settings
        required_settings = [
            ('media_source_url', 'MEDIA_SOURCE_URL'),
            ('media_source_id', 'MEDIA_SOURCE_ID'),
            ('bucket', 'BUCKET'),
            ('aws_region', 'AWS_REGION')
        ]

        missing_settings = []
        for config_key, env_var in required_settings:
            if not self.config.get(config_key):
                missing_settings.append(f"{config_key} (env: {env_var})")

        if missing_settings:
            self.logger.error("Missing required configuration:")
            for setting in missing_settings:
                self.logger.error(f"   • {setting}")
            raise ValueError(f"Missing required configuration: {', '.join(missing_settings)}")

        self.logger.info("Configuration validation passed")

    def setup_source_name(self):
        """Setup source name for logging"""
        media_source_id = self.config.get('media_source_id', 'nosource')
        scheduled_job_id = self.config.get('scheduled_job_id', 'nosj')
        self.config['source_name'] = f"{media_source_id}__{scheduled_job_id}"

    def setup_logging(self):
        """Setup container-friendly logging"""
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        self.setup_source_name()
        source_name = self.config.get('source_name', 'unknown')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        in_kubernetes = (
            os.getenv('KUBERNETES_SERVICE_HOST') is not None or
            os.getenv('CONTAINER_ENV') is not None or
            os.path.exists('/.dockerenv')
        )

        console_handler = logging.StreamHandler(sys.stdout)

        if in_kubernetes:
            formatter = logging.Formatter(
                f'%(asctime)s - %(levelname)s - Source:{source_name} - [%(threadName)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            formatter = logging.Formatter(
                f'%(asctime)s - %(name)s - %(levelname)s - Source:{source_name} - [%(threadName)s] %(message)s'
            )

        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if not in_kubernetes:
            try:
                file_handler = logging.handlers.RotatingFileHandler(
                    'stream_ingestion_engine.log',
                    maxBytes=10*1024*1024,
                    backupCount=5,
                    encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                logger.info("Local environment detected - logging to file and console")
            except Exception as e:
                logger.warning(f"Could not create file handler: {e}")
                logger.info("Using console-only logging")
        else:
            logger.info("Container environment detected - using stdout logging only")

    def log_processing_mode(self):
        """Log the current processing configuration"""
        self.logger.info("=== CONFIGURATION ===")

        # SRT settings
        if self.config.get('media_source_pass_phrase'):
            self.logger.info("SRT: Encrypted connection ENABLED")
        else:
            self.logger.info("SRT: Unencrypted connection")

        if self.config.get('srt_latency'):
            self.logger.info(f"SRT BUFFERING: {self.config['srt_latency']}ms latency buffer")

        # Recording settings
        self.logger.info("VIDEO: Copy mode (no processing)")
        self.logger.info("AUDIO RECORDING: Copy mode (sync preserved)")
        
        # Post-processing settings
        if self.config.get('audio_downmix', True):
            audio_bitrate = self.config.get('audio_bitrate', '192k')
            self.logger.info(f"AUDIO POST-PROCESS: Downmix to stereo @ {audio_bitrate} (after recording)")
        else:
            self.logger.info("AUDIO POST-PROCESS: Disabled (keeping original)")

        # Segment settings
        segment_seconds = self.config.get('segment_length_in_seconds', 300)
        output_format = self.config.get('output_format', 'mp4').lower()
        self.logger.info(f"SEGMENTS: {segment_seconds}s duration, {output_format} format")
        self.logger.info("TIMING: Using FFmpeg -strftime for automatic timestamps")

    def cleanup_old_files(self):
        """Clean up any old temp files"""
        patterns = ['temp_segment_*.ts', 'temp_segment_*.mp4']
        total_cleaned = 0

        for pattern in patterns:
            files = list(self.segments_dir.glob(pattern))
            if files:
                self.logger.info(f"Cleaning {len(files)} old {pattern} files")
                for f in files:
                    f.unlink()
                    total_cleaned += 1

        if total_cleaned > 0:
            self.logger.info(f"Cleaned up {total_cleaned} old temp files")

    def emergency_cleanup(self):
        """Emergency cleanup if disk space is critically low - for 24/7 operation"""
        try:
            # Find all segment files
            patterns = ['seg_*.ts', 'seg_*.mp4', 'temp_*.ts', 'temp_*.mp4']
            all_files = []
            
            for pattern in patterns:
                all_files.extend(list(self.segments_dir.glob(pattern)))
            
            if not all_files:
                return
            
            # Sort by modification time (oldest first)
            all_files.sort(key=lambda x: x.stat().st_mtime)
            
            # In emergency, keep only the most recent 10 files
            if len(all_files) > 10:
                files_to_delete = all_files[:-10]
                deleted_count = 0
                
                for file_path in files_to_delete:
                    try:
                        file_size_mb = file_path.stat().st_size / (1024 * 1024)
                        self.logger.warning(f"Emergency cleanup: deleting {file_path.name} ({file_size_mb:.1f}MB)")
                        file_path.unlink()
                        deleted_count += 1
                    except Exception as e:
                        self.logger.error(f"Could not delete {file_path.name}: {e}")
                
                if deleted_count > 0:
                    self.logger.critical(f"Emergency cleanup completed: deleted {deleted_count} old files to free disk space")
                    
                    # Check disk space after cleanup
                    try:
                        _, _, free = shutil.disk_usage(self.segments_dir)
                        free_gb = free // (1024**3)
                        self.logger.info(f"Disk space after emergency cleanup: {free_gb}GB free")
                    except:
                        pass
                        
        except Exception as e:
            self.logger.error(f"Emergency cleanup failed: {e}")

    def retry_failed_uploads(self):
        """Check for and retry any properly named files that failed to upload"""
        output_format = self.config.get('output_format', 'mp4').lower()

        # Only look for the expected output format
        if output_format == 'mp4':
            # Only retry MP4 files - TS files are intermediate and should be cleaned up
            pattern = f"seg_{self.config['media_source_id']}_*_*.mp4"
        else:
            # Only retry TS files if that's the output format
            pattern = f"seg_{self.config['media_source_id']}_*_*.ts"

        failed_files = list(self.segments_dir.glob(pattern))

        # Clean up any leftover intermediate TS files if output format is MP4
        if output_format == 'mp4':
            leftover_ts = list(self.segments_dir.glob(f"seg_{self.config['media_source_id']}_*_*.ts"))
            if leftover_ts:
                self.logger.warning(f"Found {len(leftover_ts)} leftover TS files (intermediate format) - deleting...")
                for ts_file in leftover_ts:
                    try:
                        # Check file age - don't delete if very recent (might be active segment)
                        file_age = time.time() - ts_file.stat().st_mtime
                        if file_age > 600:  # Older than 10 minutes = safe to delete
                            self.logger.info(f"Deleting old intermediate TS file: {ts_file.name}")
                            ts_file.unlink()
                        else:
                            self.logger.info(f"Keeping recent TS file (age: {file_age:.0f}s): {ts_file.name}")
                    except Exception as e:
                        self.logger.error(f"Could not delete {ts_file.name}: {e}")

        if failed_files:
            self.logger.info(f"Found {len(failed_files)} {output_format.upper()} files to retry upload...")
            for file_path in failed_files:
                self.upload_executor.submit(self.upload_to_s3, file_path)
        else:
            self.logger.info(f"No failed {output_format.upper()} upload files found")

    def get_srt_input_options(self):
        """Get SRT input options"""
        srt_options = []

        if self.config.get('media_source_pass_phrase'):
            encoded_passphrase = urllib.parse.quote(self.config['media_source_pass_phrase'], safe='')
            srt_options.extend(['-passphrase', encoded_passphrase])

        if self.config.get('srt_pbkeylen'):
            srt_options.extend(['-pbkeylen', str(self.config['srt_pbkeylen'])])

        if self.config.get('srt_latency'):
            srt_options.extend(['-latency', str(self.config['srt_latency'])])

        if self.config.get('srt_maxbw'):
            srt_options.extend(['-maxbw', str(self.config['srt_maxbw'])])

        if self.config.get('srt_rcvbuf'):
            srt_options.extend(['-rcvbuf', str(self.config['srt_rcvbuf'])])

        if self.config.get('srt_sndbuf'):
            srt_options.extend(['-sndbuf', str(self.config['srt_sndbuf'])])

        return srt_options

    def test_srt_connection(self, timeout_seconds=15):
        """Test SRT connection without starting full recording"""
        self.logger.info("Testing SRT connection...")
        
        srt_options = self.get_srt_input_options()
        
        cmd = ['ffmpeg', '-y', '-v', 'error']
        cmd.extend(srt_options)
        cmd.extend(['-i', self.config['media_source_url']])
        cmd.extend(['-t', '3', '-f', 'null', '-'])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
            if result.returncode == 0:
                self.logger.info("SRT connection test successful")
                return True
            else:
                self.logger.warning(f"SRT connection failed: {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            self.logger.warning("SRT connection test timed out")
            return False
        except Exception as e:
            self.logger.warning(f"SRT connection test error: {e}")
            return False

    def log_available_streams(self):
        """List all available video and audio streams with indices"""
        try:
            self.logger.info("=== DETECTING AVAILABLE STREAMS ===")
            srt_options = self.get_srt_input_options()

            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams']
            cmd.extend(srt_options)
            cmd.append(self.config['media_source_url'])

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode != 0:
                self.logger.warning("Could not probe streams")
                return

            probe_data = json.loads(result.stdout)
            streams = probe_data.get('streams', [])
            
            video_count = 0
            audio_count = 0
            
            for stream in streams:
                index = stream.get('index', '?')
                codec_type = stream.get('codec_type', 'unknown')
                codec_name = stream.get('codec_name', 'unknown')
                
                if codec_type == 'video':
                    width = stream.get('width', '?')
                    height = stream.get('height', '?')
                    fps = stream.get('r_frame_rate', '?')
                    self.logger.info(f"VIDEO Stream {index}: {codec_name} {width}x{height} @ {fps} fps")
                    video_count += 1
                elif codec_type == 'audio':
                    channels = stream.get('channels', '?')
                    sample_rate = stream.get('sample_rate', '?')
                    self.logger.info(f"AUDIO Stream {index}: {codec_name} {channels}ch @ {sample_rate}Hz")
                    audio_count += 1
            
            self.logger.info(f"Total: {video_count} video, {audio_count} audio streams")
            self.logger.info("=================================")
            
            # Now show what we're actually using
            self.log_stream_selection()
            
        except Exception as e:
            self.logger.warning(f"Could not list streams: {e}")

    def log_stream_selection(self):
        """Log which streams will be used based on config"""
        self.logger.info("=== STREAM SELECTION ===")
        
        video_index = self.config.get('video_stream_index', 0)
        audio_config = self.config.get('audio_stream_index', 1)
        
        # Show config source for video
        if 'video_stream_index' in self.config:
            self.logger.info(f"VIDEO: Using stream {video_index} (from config)")
        else:
            self.logger.info(f"VIDEO: Using stream {video_index} (default)")
        
        # Determine actual audio index that will be used
        if audio_config == 'auto':
            actual_audio_index = 1  # Auto defaults to stream 1
            self.logger.info(f"AUDIO: Using stream {actual_audio_index} (auto-detected, defaults to first audio)")
        elif 'audio_stream_index' in self.config:
            actual_audio_index = int(audio_config) if audio_config is not None else 1
            self.logger.info(f"AUDIO: Using stream {actual_audio_index} (from config)")
        else:
            actual_audio_index = 1
            self.logger.info(f"AUDIO: Using stream {actual_audio_index} (default)")
        
        self.logger.info(f"FFmpeg will use: -map 0:{video_index} -map 0:{actual_audio_index}")
        self.logger.info("========================")

    def auto_detect_streams(self):
        """Auto-detect video and audio stream indices"""
        try:
            srt_options = self.get_srt_input_options()
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams']
            cmd.extend(srt_options)
            cmd.append(self.config['media_source_url'])

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                probe_data = json.loads(result.stdout)
                streams = probe_data.get('streams', [])

                video_index = None
                audio_index = None

                # Find first video and first audio stream
                for stream in streams:
                    codec_type = stream.get('codec_type')
                    index = stream.get('index')

                    if codec_type == 'video' and video_index is None:
                        video_index = index
                    elif codec_type == 'audio' and audio_index is None:
                        audio_index = index

                    # Stop once we found both
                    if video_index is not None and audio_index is not None:
                        break

                return video_index, audio_index
        except Exception as e:
            self.logger.warning(f"Stream auto-detection failed: {e}")
            return None, None

    def detect_best_audio_stream(self):
        """Detect audio stream with config override support"""
        # Check if manually specified in config
        if 'audio_stream_index' in self.config:
            audio_config = self.config['audio_stream_index']

            # If it's "auto" or None, skip to auto-detection
            if audio_config != 'auto' and audio_config is not None:
                self.logger.info(f"Using manually specified audio stream {audio_config} (from config)")
                return int(audio_config)

        # Auto-detect
        _, audio_index = self.auto_detect_streams()

        if audio_index is not None:
            self.logger.info(f"Auto-detected audio stream: {audio_index}")
            return audio_index

        # Fallback
        self.logger.warning("Audio auto-detection failed, defaulting to stream 1")
        return 1

    def detect_best_video_stream(self):
        """Detect video stream with config override support"""
        # Check if manually specified in config
        if 'video_stream_index' in self.config:
            video_config = self.config['video_stream_index']

            # If it's "auto" or None, skip to auto-detection
            if video_config != 'auto' and video_config is not None:
                self.logger.info(f"Using manually specified video stream {video_config} (from config)")
                return int(video_config)

        # Auto-detect
        video_index, _ = self.auto_detect_streams()

        if video_index is not None:
            self.logger.info(f"Auto-detected video stream: {video_index}")
            return video_index

        # Fallback
        self.logger.warning("Video auto-detection failed, defaulting to stream 0")
        return 0

    def get_stream_mapping(self):
        """Get video and audio stream mapping based on config or auto-detection"""
        video_index = self.detect_best_video_stream()
        audio_index = self.detect_best_audio_stream()

        self.logger.info(f"Final mapping -> video: 0:{video_index}, audio: 0:{audio_index}")
        return video_index, audio_index

    def start_ffmpeg_with_strftime(self, segment_seconds):
        """Start FFmpeg with strftime for automatic timestamping - ALWAYS OUTPUT TS"""
        video_index, audio_index = self.get_stream_mapping()
        srt_options = self.get_srt_input_options()
        
        media_source = self.config['media_source_id']
        
        cmd = [
            'ffmpeg', '-y',
            '-thread_queue_size', '2048',
            '-fflags', '+genpts+discardcorrupt',  # Generate PTS if missing, discard corrupt packets
        ]

        cmd.extend(srt_options)
        cmd.extend(['-i', self.config['media_source_url']])

        cmd.extend([
            '-map', f'0:{video_index}',
            '-map', f'0:{audio_index}',
            '-c:v', 'copy',  # Always copy video
            '-c:a', 'copy',  # ALWAYS COPY AUDIO for perfect sync
            '-f', 'segment',
            '-segment_time', str(segment_seconds),
            '-strftime', '1',  # KEY: Automatic timestamping
            '-segment_format', 'mp4',  # Direct MP4 output
            '-segment_format_options', 'movflags=+faststart',  # Optimize for streaming
            '-reset_timestamps', '1',  # Reset timestamps for each segment
            '-segment_wrap', '999999',  # Prevent number overflow
            '-avoid_negative_ts', 'make_zero',  # Handle timestamp issues
            '-max_muxing_queue_size', '4096',  # Larger queue for stability
        ])

        # Direct MP4 output
        output_pattern = str(self.segments_dir / f'seg_{media_source}_%s_{segment_seconds}.mp4')
        cmd.append(output_pattern)
        
        self.logger.info(f"Starting FFmpeg with strftime - {segment_seconds}s MP4 segments")
        self.logger.info("Audio: COPY mode (no transcoding - sync preserved)")
        self.logger.info("Video: COPY mode (no transcoding - quality preserved)")
        self.logger.info("Format: Direct MP4 output (no conversion needed)")
        self.logger.info(f"Output pattern: seg_{media_source}_EPOCH_{segment_seconds}.mp4")
        
        self.ffmpeg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        threading.Thread(target=self._monitor_ffmpeg_output, daemon=True).start()
        return self.ffmpeg_process

    def start_ffmpeg_with_retry(self, segment_seconds):
        """Start FFmpeg with robust connection retry logic"""
        for attempt in range(self.max_connection_retries):
            attempt_num = attempt + 1
            self.logger.info(f"Starting FFmpeg connection (attempt {attempt_num}/{self.max_connection_retries})")
            
            if not self.test_srt_connection():
                self.logger.warning(f"Connection test failed on attempt {attempt_num}")
            else:
                try:
                    process = self.start_ffmpeg_with_strftime(segment_seconds)
                    
                    if process:
                        self.logger.info("Waiting for FFmpeg to stabilize...")
                        time.sleep(15)
                        
                        if process.poll() is None:
                            self.logger.info("FFmpeg connection established successfully")
                            if attempt > 0:
                                self.stats['reconnections'] += 1
                                self.logger.info(f"Successfully reconnected after {attempt} failed attempts")
                            return process
                        else:
                            self.logger.warning(f"FFmpeg process died immediately on attempt {attempt_num}")
                            
                except Exception as e:
                    self.logger.error(f"FFmpeg start failed on attempt {attempt_num}: {e}")
            
            self.stats['connection_retries'] += 1
            
            if attempt < self.max_connection_retries - 1:
                retry_delay = min(
                    self.base_retry_delay * (2 ** attempt),
                    self.max_retry_delay
                )
                self.logger.info(f"Waiting {retry_delay}s before retry...")
                
                for _ in range(int(retry_delay)):
                    if not self.running:
                        return None
                    time.sleep(1)
            else:
                self.logger.error(f"All {self.max_connection_retries} connection attempts failed")
        
        return None

    def post_process_segment(self, segment_path):
        """Post-process segment: downmix if needed (file is already MP4 from FFmpeg)"""
        final_path = segment_path

        # Determine if we need audio processing
        needs_downmix = self.config.get('audio_downmix', True)

        # File is already MP4 from FFmpeg - skip if no downmix needed
        if not needs_downmix:
            self.logger.info(f"Segment ready (no downmix needed): {final_path.name}")
            return final_path

        # Downmix the MP4 file (file is already MP4, just need to downmix audio)
        if final_path.suffix == '.mp4' and needs_downmix:
            try:
                # Create temp file to avoid input/output conflict
                temp_downmixed = final_path.parent / f"{final_path.stem}_downmix_temp.mp4"

                cmd = [
                    'ffmpeg', '-y', '-v', 'error',
                    '-i', str(final_path),
                    '-c:v', 'copy',  # Keep video as-is
                    '-c:a', 'aac',  # Re-encode audio to AAC stereo
                    '-ac', '2',  # Downmix to stereo
                    '-b:a', self.config.get('audio_bitrate', '192k'),
                    '-ar', '48000',
                    '-movflags', '+faststart',
                    '-max_muxing_queue_size', '1024',
                    str(temp_downmixed)
                ]

                self.logger.info(f"Downmixing audio in MP4 segment: {final_path.name}")

                # Timeout: 120s for 5-min segments
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

                if result.returncode == 0 and temp_downmixed.exists() and temp_downmixed.stat().st_size > 0:
                    # Success - replace original with downmixed version
                    original_size = final_path.stat().st_size / 1024 / 1024
                    downmixed_size = temp_downmixed.stat().st_size / 1024 / 1024

                    final_path.unlink()  # Delete original
                    temp_downmixed.rename(final_path)  # Rename temp to final

                    self.stats['segments_downmixed'] += 1
                    self.logger.info(f"MP4 downmix complete: {final_path.name} ({original_size:.1f}MB → {downmixed_size:.1f}MB)")
                else:
                    # Downmix failed
                    self.logger.error(f"MP4 downmix FAILED (exit code {result.returncode}) - deleting corrupted segment")
                    if result.stderr:
                        for err_line in result.stderr.split('\n'):
                            if err_line.strip():
                                self.logger.error(f"  FFmpeg: {err_line.strip()}")

                    # Clean up
                    if temp_downmixed.exists():
                        temp_downmixed.unlink()
                    if final_path.exists():
                        final_path.unlink()

                    self.stats['upload_failures'] += 1
                    return None

            except subprocess.TimeoutExpired:
                self.logger.error(f"MP4 downmix TIMEOUT for {final_path.name} - deleting corrupted segment")
                if temp_downmixed.exists():
                    temp_downmixed.unlink()
                if final_path.exists():
                    final_path.unlink()
                self.stats['upload_failures'] += 1
                return None
            except Exception as e:
                self.logger.error(f"MP4 downmix ERROR for {final_path.name}: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                if 'temp_downmixed' in locals() and temp_downmixed.exists():
                    temp_downmixed.unlink()
                if final_path.exists():
                    final_path.unlink()
                self.stats['upload_failures'] += 1
                return None

        return final_path

    def process_and_upload_segment(self, file_path):
        """Process segment (downmix and/or convert) then upload"""
        try:
            # Verify file exists and is readable before processing
            if not file_path.exists():
                self.logger.error(f"Segment file disappeared before processing: {file_path.name}")
                return

            # Additional safety: try to open file to ensure it's not locked
            try:
                with open(file_path, 'rb') as f:
                    f.read(1)  # Read 1 byte to verify file is accessible
            except Exception as e:
                self.logger.error(f"Cannot access segment file {file_path.name}: {e}")
                return

            # Post-process: downmix and/or convert format
            processed_path = self.post_process_segment(file_path)

            # Upload to S3 only if processing succeeded
            if processed_path and processed_path.exists():
                self.upload_to_s3(processed_path)
            elif processed_path is None:
                # Processing failed - file preserved but upload skipped
                self.logger.warning(f"Skipping upload for {file_path.name} due to processing failure")

        except Exception as e:
            self.logger.error(f"Error processing segment {file_path.name}: {e}")

    def monitor_segments_simplified(self, segment_seconds):
        """Simplified monitoring - wait for FFmpeg to finish writing segments"""
        self.logger.info("Starting simplified segment monitoring with strftime...")

        # Monitor for MP4 files (direct output from FFmpeg)
        pattern = f'seg_{self.config["media_source_id"]}_*_{segment_seconds}.mp4'

        processed_files = set()
        pending_files = {}  # file_path -> {'first_seen': time, 'last_mtime': time, 'stable_since': time}
        last_activity = time.time()
        stall_timeout = segment_seconds + 60

        while self.running:
            # Check connection health
            if not self.ffmpeg_process or self.ffmpeg_process.poll() is not None:
                self.logger.error("FFmpeg died - attempting reconnection...")

                self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_seconds)
                if not self.ffmpeg_process:
                    self.logger.error("Could not reconnect - ending recording")
                    break

                last_activity = time.time()
                continue

            current_time = time.time()

            # Find all segment files
            current_files = set(self.segments_dir.glob(pattern))

            # Check for new files
            new_files = current_files - processed_files - set(pending_files.keys())
            if new_files:
                last_activity = current_time
                for file_path in new_files:
                    if file_path.stat().st_size > 100000:  # Basic size check
                        pending_files[file_path] = {
                            'first_seen': current_time,
                            'last_mtime': file_path.stat().st_mtime,
                            'stable_since': None
                        }
                        self.logger.info(f"Detected new segment (waiting for completion): {file_path.name}")

            # Check pending files for completion
            completed_files = []
            for file_path, info in pending_files.items():
                if not file_path.exists():
                    continue

                try:
                    current_mtime = file_path.stat().st_mtime

                    # If modification time changed, file is still being written
                    if current_mtime != info['last_mtime']:
                        info['last_mtime'] = current_mtime
                        info['stable_since'] = None
                        continue

                    # If modification time is stable, start tracking stability
                    if info['stable_since'] is None:
                        info['stable_since'] = current_time
                        continue

                    # File has been stable for at least 30 seconds - consider it complete
                    stable_duration = current_time - info['stable_since']
                    if stable_duration >= 30:
                        # Additional check: ensure file age makes sense for segment duration
                        file_age = current_time - info['first_seen']
                        expected_min_age = segment_seconds - 30  # Allow some tolerance

                        if file_age >= expected_min_age or file_age >= 60:  # Minimum 60s for shorter segments
                            completed_files.append(file_path)

                            # Extract epoch timestamp from filename for logging
                            try:
                                parts = file_path.stem.split('_')
                                if len(parts) >= 4:
                                    epoch_str = parts[-2]
                                    epoch_ts = int(epoch_str)
                                    readable_time = datetime.fromtimestamp(epoch_ts, tz=timezone.utc).strftime('%H:%M:%S UTC')
                                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                                    self.logger.info(f"Segment completed: {file_path.name} (time: {readable_time}, size: {file_size_mb:.1f}MB, age: {file_age:.0f}s)")
                            except:
                                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                                self.logger.info(f"Segment completed: {file_path.name} (size: {file_size_mb:.1f}MB, age: {file_age:.0f}s)")
                        else:
                            self.logger.debug(f"File {file_path.name} stable but too young ({file_age:.0f}s < {expected_min_age}s)")

                except Exception as e:
                    self.logger.warning(f"Error checking file {file_path.name}: {e}")
                    # Remove problematic file from pending
                    completed_files.append(file_path)

            # Process completed files
            for file_path in completed_files:
                pending_files.pop(file_path, None)
                processed_files.add(file_path)
                self.stats['segments_created'] += 1
                last_activity = current_time

                # Queue for post-processing and upload
                self.process_executor.submit(self.process_and_upload_segment, file_path)

                # Prevent memory leak
                if len(processed_files) > 100:
                    oldest = sorted(processed_files)[:50]
                    for old_file in oldest:
                        processed_files.discard(old_file)

            # Clean up stale pending files (older than 2x segment duration)
            stale_files = []
            for file_path, info in pending_files.items():
                if current_time - info['first_seen'] > (segment_seconds * 2):
                    self.logger.warning(f"Removing stale pending file: {file_path.name}")
                    stale_files.append(file_path)

            for file_path in stale_files:
                pending_files.pop(file_path, None)

            # Stall detection
            time_since_activity = current_time - last_activity
            if time_since_activity > stall_timeout:
                self.logger.warning(f"No new segments for {stall_timeout}s - reconnecting...")

                if self.ffmpeg_process:
                    self.ffmpeg_process.terminate()
                    try:
                        self.ffmpeg_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.ffmpeg_process.kill()

                self.stats['stall_recoveries'] += 1
                last_activity = current_time

            time.sleep(10)

        self.logger.info("Monitoring ended")

    def _monitor_ffmpeg_output(self):
        """Monitor FFmpeg stderr with intelligent filtering and rotation"""
        if not self.ffmpeg_process:
            return

        self.logger.info("Starting FFmpeg monitoring with intelligent filtering")

        ffmpeg_log_file = self.segments_dir / 'ffmpeg.log'

        # Rotate log if too large (20MB per file)
        if ffmpeg_log_file.exists() and ffmpeg_log_file.stat().st_size > 20 * 1024 * 1024:
            # Keep 3 rotated logs (20MB each = 80MB max total)
            for i in range(2, 0, -1):
                old_log = self.segments_dir / f'ffmpeg.log.{i}'
                if old_log.exists():
                    if i == 2:
                        old_log.unlink()  # Delete oldest
                    else:
                        old_log.rename(self.segments_dir / f'ffmpeg.log.{i+1}')

            ffmpeg_log_file.rename(self.segments_dir / 'ffmpeg.log.1')
            self.logger.info("Rotated FFmpeg log file")

        last_speed_log = 0
        perf_warning_threshold = self.config.get('performance_warning_threshold', 0.95)
        ffmpeg_start_time = time.time()  # Track startup time

        # Intelligent error throttling - prevents log spam
        error_counts = {}  # error_pattern -> count
        last_error_summary = time.time()
        summary_interval = 300  # Summarize errors every 5 minutes

        try:
            with open(ffmpeg_log_file, 'a', buffering=1) as ffmpeg_log:  # Line buffered
                ffmpeg_log.write(f"\n{'='*80}\n")
                ffmpeg_log.write(f"FFmpeg Session Started: {datetime.now(timezone.utc).isoformat()}\n")
                ffmpeg_log.write(f"{'='*80}\n")

                for line in iter(self.ffmpeg_process.stderr.readline, ''):
                    if not self.running or not self.ffmpeg_process:
                        break

                    line = line.strip()
                    if not line:
                        continue

                    current_time = time.time()

                    # Categorize the line
                    is_noise = False
                    error_pattern = None

                    # Identify noisy but expected errors (count but don't log each one)
                    if any(pattern in line.lower() for pattern in [
                        'timestamp discontinuity',
                        'non-monotonic dts',
                        'packet corrupt',
                        'pes packet size mismatch',
                        'decode_slice_header error'
                    ]):
                        is_noise = True
                        # Extract pattern for counting
                        if 'timestamp discontinuity' in line.lower():
                            error_pattern = 'timestamp_discontinuity'
                        elif 'non-monotonic dts' in line.lower():
                            error_pattern = 'non_monotonic_dts'
                        elif 'packet corrupt' in line.lower():
                            error_pattern = 'packet_corrupt'
                        else:
                            error_pattern = 'other_noise'

                        error_counts[error_pattern] = error_counts.get(error_pattern, 0) + 1

                    # Always write to file, but selectively (skip some noise)
                    if not is_noise or error_counts.get(error_pattern, 0) <= 10:
                        # Log first 10 of each error type, then start sampling
                        ffmpeg_log.write(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {line}\n")
                    elif error_counts.get(error_pattern, 0) % 100 == 0:
                        # Log every 100th occurrence after first 10
                        ffmpeg_log.write(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {line} [occurred {error_counts[error_pattern]} times]\n")

                    # Log important events to main application log
                    if 'speed=' in line:
                        try:
                            speed_part = [part for part in line.split() if 'speed=' in part][0]
                            speed_value = float(speed_part.replace('speed=', '').replace('x', ''))

                            # Ignore speed warnings during first 60 seconds (startup/buffering)
                            time_since_start = current_time - ffmpeg_start_time
                            if time_since_start > 60 and speed_value < perf_warning_threshold:
                                self.logger.warning(f"FFmpeg performance degraded: speed={speed_value:.2f}x")
                            elif current_time - last_speed_log > 600:
                                self.logger.debug(f"FFmpeg speed: {speed_value:.2f}x")
                                last_speed_log = current_time
                        except:
                            pass

                    elif 'opening' in line.lower() and 'for writing' in line.lower():
                        self.logger.debug("FFmpeg: Opening new segment file")

                    elif 'error' in line.lower() and not is_noise:
                        # Real, unexpected errors - always log to main log
                        self.logger.error(f"FFmpeg: {line}")

                    # Periodic error summary
                    if current_time - last_error_summary > summary_interval and error_counts:
                        summary_lines = [f"FFmpeg error summary (last 5 min):"]
                        for pattern, count in error_counts.items():
                            summary_lines.append(f"  {pattern}: {count} occurrences")

                        summary = " | ".join(summary_lines)
                        self.logger.info(summary)
                        ffmpeg_log.write(f"\n{datetime.now(timezone.utc).strftime('%H:%M:%S')} === ERROR SUMMARY ===\n")
                        for pattern, count in error_counts.items():
                            ffmpeg_log.write(f"  {pattern}: {count} occurrences\n")
                        ffmpeg_log.write(f"{'='*40}\n\n")

                        error_counts.clear()
                        last_error_summary = current_time

        except Exception as e:
            if self.running:
                self.logger.error(f"Error monitoring FFmpeg output: {e}")
        finally:
            self.logger.info("FFmpeg monitoring thread ending")

    def upload_to_s3(self, file_path):
        """Upload file to S3 with retry logic"""
        for attempt in range(3):
            try:
                s3_key = f"{self.config['bucket_path']}/{file_path.name}"
                
                # Determine content type
                if file_path.suffix.lower() == '.ts':
                    content_type = 'video/mp2t'
                else:
                    content_type = 'video/mp4'
                
                self.logger.info(f"Uploading {file_path.name} to {s3_key} (attempt {attempt + 1})")
                
                self.s3.upload_file(
                    str(file_path),
                    self.config['bucket'],
                    s3_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': {
                            'media_source_id': str(self.config['media_source_id']),
                            'scheduled_job_id': str(self.config['scheduled_job_id']),
                            'upload_time': datetime.now(timezone.utc).isoformat(),
                            'recorder_version': 'v5-postprocess-strftime'
                        }
                    }
                )
                
                self.logger.info(f"Successfully uploaded {file_path.name}")
                self.stats['segments_uploaded'] += 1

                # Only delete file after successful upload
                file_path.unlink()
                return True

            except Exception as e:
                self.logger.error(f"Upload attempt {attempt + 1} failed for {file_path.name}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))

        self.logger.error(f"All upload attempts failed for {file_path.name} - file preserved for retry")
        self.stats['upload_failures'] += 1
        return False

    def wait_for_start_time(self):
        """Wait until scheduled start time if configured"""
        if self.config.get('start_date_time'):
            start_epoch = float(self.config['start_date_time'])
            scheduled_start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
            current_time = datetime.now(timezone.utc)
            
            if scheduled_start > current_time:
                wait_seconds = (scheduled_start - current_time).total_seconds()
                self.logger.info(f"Scheduled start: {scheduled_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                self.logger.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                self.logger.info(f"Waiting {wait_seconds:.0f} seconds...")
                
                while wait_seconds > 0 and self.running:
                    time.sleep(min(wait_seconds, 1.0))
                    wait_seconds -= 1
                    
                    if int(wait_seconds) % 30 == 0 and wait_seconds > 0:
                        self.logger.info(f"  {int(wait_seconds)} seconds until start...")
                
                self.recording_start_time = scheduled_start
            else:
                self.logger.info("Scheduled time already passed, starting immediately")
                self.recording_start_time = current_time
        else:
            self.logger.info("No scheduled start time - beginning immediately")
            self.recording_start_time = datetime.now(timezone.utc)

    def shutdown(self, signum, _frame):
        """Graceful shutdown"""
        self.logger.info(f"Shutting down (signal {signum})...")
        self.running = False
        
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()
        
        self.process_executor.shutdown(wait=True)
        self.upload_executor.shutdown(wait=True)
        
        # Log final statistics
        self.logger.info("=== Final Statistics ===")
        self.logger.info(f"Segments created: {self.stats['segments_created']}")
        self.logger.info(f"Segments downmixed: {self.stats['segments_downmixed']}")
        self.logger.info(f"Segments uploaded: {self.stats['segments_uploaded']}")
        self.logger.info(f"Upload failures: {self.stats['upload_failures']}")
        self.logger.info(f"Connection retries: {self.stats['connection_retries']}")
        self.logger.info(f"Reconnections: {self.stats['reconnections']}")
        self.logger.info(f"Stall recoveries: {self.stats['stall_recoveries']}")
        
        if self.stats['recording_start_time']:
            duration = datetime.now(timezone.utc) - self.stats['recording_start_time']
            hours = int(duration.total_seconds() // 3600)
            minutes = int((duration.total_seconds() % 3600) // 60)
            self.logger.info(f"Total recording time: {hours}h {minutes}m")
        
        self.logger.info("Shutdown complete")
        sys.exit(0)

    def record_continuous(self):
        """Record continuously with simplified timing using strftime"""
        segment_seconds = self.config.get('segment_length_in_seconds', 300)
        if segment_seconds <= 0:
            segment_seconds = 300
        
        self.logger.info(f"Starting continuous recording with {segment_seconds}-second segments")
        self.logger.info("Using FFmpeg -strftime for automatic timestamping")
        self.logger.info("Audio will be copied during recording, downmixed after if enabled")
        
        # Log available streams for debugging (from old code)
        self.log_available_streams()
        
        # Wait for scheduled start if configured
        self.wait_for_start_time()
        
        self.stats['recording_start_time'] = self.recording_start_time
        self.logger.info(f"Recording started at: {self.recording_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # Start FFmpeg with strftime
        self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_seconds)
        
        if not self.ffmpeg_process:
            self.logger.error("Could not establish initial connection - exiting")
            return
        
        # Start simplified monitoring thread
        monitor_thread = threading.Thread(
            target=self.monitor_segments_simplified,
            args=(segment_seconds,),
            daemon=True,
            name="monitor_segments"
        )
        monitor_thread.start()
        
        # Main loop - monitor health and log stats periodically
        try:
            stats_counter = 0
            disk_check_counter = 0
            
            while self.running:
                time.sleep(60)  # Check every minute
                stats_counter += 60
                disk_check_counter += 60
                
                # Log stats every 5 minutes
                if stats_counter >= 300:
                    self.logger.info(
                        f"Stats [24/7 Operation]: "
                        f"Created: {self.stats['segments_created']}, "
                        f"Downmixed: {self.stats['segments_downmixed']}, "
                        f"Uploaded: {self.stats['segments_uploaded']}, "
                        f"Failed: {self.stats['upload_failures']}, "
                        f"Reconnections: {self.stats.get('reconnections', 0)}"
                    )
                    stats_counter = 0
                
                # Check disk space every 10 minutes
                if disk_check_counter >= 600:
                    try:
                        total, used, free = shutil.disk_usage(self.segments_dir)
                        free_gb = free // (1024**3)
                        used_percent = (used / total) * 100

                        if free_gb < 1:
                            self.logger.critical(f"CRITICAL: Only {free_gb}GB disk space remaining! Recording may fail!")
                            # Emergency cleanup when critically low
                            self.emergency_cleanup()
                        elif free_gb < 5:
                            self.logger.warning(f"WARNING: Low disk space: {free_gb}GB remaining ({used_percent:.1f}% used)")
                        else:
                            self.logger.info(f"Disk space OK: {free_gb}GB free ({used_percent:.1f}% used)")
                    except Exception as e:
                        self.logger.warning(f"Could not check disk space: {e}")

                    disk_check_counter = 0
                    
        except KeyboardInterrupt:
            self.logger.info("Recording stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
        
        self.running = False
        monitor_thread.join(timeout=30)


def main():
    parser = argparse.ArgumentParser(description='Stream Ingestion Engine V5')
    parser.add_argument('--config', default='test_config.json', help='Config file path')
    args = parser.parse_args()
    
    try:
        recorder = StreamIngestionEngineV5(args.config)
        recorder.record_continuous()
    except Exception as e:
        stack_trace = traceback.format_exc()
        print(stack_trace)
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()