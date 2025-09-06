#!/usr/bin/env python3
"""
Unified SRT Recorder V3 - Enhanced with Stall Detection, Network Stability & K8s Environment Variables
Production version with aggressive 5-minute segmentation, smart audio/video handling,
automatic SRT reconnection, advanced stall detection, and comprehensive K8s environment support

NEW FEATURES IN V3:
- Advanced stall detection (detects hung FFmpeg processes)
- Enhanced SRT buffering support for network stability
- Improved segment processing with duplicate protection
- Better error recovery and connection monitoring
- Optimized for unreliable network conditions
- COMPREHENSIVE K8S ENVIRONMENT VARIABLE SUPPORT (22 variables)
"""

import subprocess
import time
import json
import boto3
import threading
import shutil
import tempfile
import traceback
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import signal
import sys
import logging
import logging.handlers
import argparse
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

DEFAULT_SEGMENTS_DIR = '/app/segments'
class UnifiedSRTRecorderV3:
    def __init__(self, config_file='test_config.json'):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        self.recording_start_time = None

        with open(config_file) as f:
            self.config = json.load(f)

        # =====================================
        # COMPREHENSIVE ENVIRONMENT VARIABLE SUPPORT FOR K8S
        # =====================================
        self.logger.info("🐳 Checking for Kubernetes/Docker environment overrides...")

        # === SRT CONNECTION SETTINGS ===
        env_overrides = {
            # Core connection
            'MEDIA_SOURCE_ID': ('media_source_id', str),
            'SCHEDULED_JOB_ID': ('scheduled_job_id', str),
            'MEDIA_SOURCE_URL': ('media_source_url', str),
            "URL": ('media_source_url', str),
            'MEDIA_SOURCE_PASS_PHRASE': ('media_source_pass_phrase', str),
            'START_DATE_TIME': ('start_date_time', float),
            'STOP_DATE_TIME': ('stop_date_time', float),
            'SEGMENT_LENGTH_IN_SECONDS': ('segment_length_in_seconds', int),
            'LOCAL_SEGMENTS_DIR': ('local_segments_dir', str),

            # SRT performance tuning
            'SRT_LATENCY': ('srt_latency', int),
            'SRT_MAXBW': ('srt_maxbw', int),
            'SRT_RCVBUF': ('srt_rcvbuf', int),
            'SRT_SNDBUF': ('srt_sndbuf', int),
            'SRT_PBKEYLEN': ('srt_pbkeylen', int),
        }

        # === AWS/S3 SETTINGS ===
        aws_overrides = {
            'BUCKET': ('bucket', str),
            'BUCKET_PATH': ('bucket_path', str),
            'AWS_REGION': ('aws_region', str),
        }

        # === PROCESSING SETTINGS ===
        processing_overrides = {
            # Video processing
            'VIDEO_PROCESS': ('video_process', lambda x: x.lower() == 'true'),
            'VIDEO_CRF': ('video_crf', int),
            'VIDEO_BITRATE': ('video_bitrate', str),
            'VIDEO_PRESET': ('video_preset', str),
            'VIDEO_FPS': ('video_fps', int),
            'VIDEO_WIDTH': ('video_width', int),
            'VIDEO_HEIGHT': ('video_height', int),

            # Audio processing
            'AUDIO_DOWNMIX': ('audio_downmix', lambda x: x.lower() == 'true'),
            'AUDIO_BITRATE': ('audio_bitrate', str),
            'AUDIO_STREAM_INDEX': ('audio_stream_index', str),  # Can be "auto" or number
            'VIDEO_STREAM_INDEX': ('video_stream_index', int),
        }

        # === PERFORMANCE/RETRY SETTINGS ===
        performance_overrides = {
            'MAX_CONNECTION_RETRIES': ('max_connection_retries', int),
            'BASE_RETRY_DELAY_SECONDS': ('base_retry_delay_seconds', int),
            'MAX_RETRY_DELAY_SECONDS': ('max_retry_delay_seconds', int),
            'PERFORMANCE_WARNING_THRESHOLD': ('performance_warning_threshold', float),
        }

        # Combine all override categories
        all_overrides = {**env_overrides, **aws_overrides, **processing_overrides, **performance_overrides}

        # Apply environment variable overrides
        overrides_applied = []

        for env_var, (config_key, converter) in all_overrides.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                try:
                    # Convert the environment variable to the correct type
                    converted_value = converter(env_value)

                    # Store original value for logging
                    original_value = self.config.get(config_key, 'NOT_SET')

                    # Override the config
                    self.config[config_key] = converted_value

                    overrides_applied.append(f"{env_var}={env_value} → {config_key}")
                    self.logger.info(f"🔧 ENV OVERRIDE: {env_var}={env_value} (was: {original_value})")

                except (ValueError, TypeError) as e:
                    self.logger.error(f"❌ Invalid environment variable {env_var}={env_value}: {e}")
                    self.logger.error(f"   Expected type: {converter}")

        # Log summary of environment overrides
        if overrides_applied:
            self.logger.info(f"📦 Applied {len(overrides_applied)} environment overrides:")
            for override in overrides_applied:
                self.logger.info(f"   • {override}")
        else:
            self.logger.info("📦 No environment overrides found - using config file values")

        # Legacy support for original environment variables (backward compatibility)
        if os.getenv('URL'):  # Original env var name
            self.config['media_source_url'] = os.getenv('URL')
            self.logger.info(f"🔄 LEGACY: Using URL environment variable (consider using media_source_url)")

        # === VALIDATION OF CRITICAL SETTINGS ===
        self.validate_configuration()

        # AWS clients
        self.s3 = boto3.client('s3', region_name=self.config['aws_region'])

        if self.config.get('bucket_path') is None:
             self.config['bucket_path'] = "processing/continuous"
        # File system
        if  self.config.get('local_segments_dir') is None:
            self.config['local_segments_dir'] = DEFAULT_SEGMENTS_DIR

        self.segments_dir = Path(self.config['local_segments_dir'])
        self.segments_dir.mkdir(exist_ok=True, parents=True)

        # Resource management
        self.upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="S3Upload")
        self.processed_segments = set()

        # Process management
        self.running = True
        self.ffmpeg_process = None
        self.recording_mode = None

        # Connection retry configuration
        self.max_connection_retries = self.config.get('max_connection_retries', 5)
        self.base_retry_delay = self.config.get('base_retry_delay_seconds', 30)
        self.max_retry_delay = self.config.get('max_retry_delay_seconds', 300)

        # Statistics
        self.stats = {
            'segments_created': 0,
            'segments_uploaded': 0,
            'upload_failures': 0,
            'connection_retries': 0,
            'reconnections': 0,
            'stall_recoveries': 0,
            'recording_start_time': None,
            'process_start_time':  datetime.now(timezone.utc)
        }

        # Signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

        self.logger.info("Unified SRT Recorder V3 with Enhanced K8s Environment Support initialized")
        self.log_processing_mode()
        self.cleanup_temp_files()
        self.retry_failed_uploads()

    def validate_configuration(self):
        """Validate critical configuration settings"""
        self.logger.info("🔍 Validating configuration...")

        # Required settings
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
            self.logger.error("❌ Missing required configuration:")
            for setting in missing_settings:
                self.logger.error(f"   • {setting}")
            raise ValueError(f"Missing required configuration: {', '.join(missing_settings)}")

        # Validate SRT URL format
        media_source_url = self.config['media_source_url']

        # TODO supporting more than just srt - e.g. rtsp?
        #if not media_source_url.startswith('srt://'):
        #    self.logger.error(f"❌ Invalid SRT URL format: {media_source_url} (must start with srt://)")
        #    raise ValueError(f"Invalid SRT URL format: {media_source_url}")

        # Validate performance threshold
        perf_threshold = self.config.get('performance_warning_threshold', 0.75)
        if not 0.1 <= perf_threshold <= 1.0:
            self.logger.warning(f"⚠️ Performance threshold {perf_threshold} outside recommended range (0.1-1.0)")

        # Validate retry settings
        max_retries = self.config.get('max_connection_retries', 5)
        if max_retries < 1 or max_retries > 20:
            self.logger.warning(f"⚠️ Max retries {max_retries} outside recommended range (1-20)")

        # setting up scheduled job id
        scheduled_job_id = self.config.get('scheduled_job_id', None)
        if scheduled_job_id is None:
            self.config['scheduled_job_id'] = "NOSJ"

        if self.config['segment_length_in_seconds'] is None:
            self.config['segment_length_in_seconds'] = 300

        self.logger.info("✅ Configuration validation passed")

    def setup_logging(self):
        """Setup rotating log files"""
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            'srt_recorder_v3.log',
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )

        console_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    def log_processing_mode(self):
        """Log the current processing configuration"""
        video_process = self.config.get('video_process', False)
        audio_downmix = self.config.get('audio_downmix', False)

        self.logger.info("=== PROCESSING CONFIGURATION V3 ===")

        # SRT Connection settings with enhanced buffering
        media_source_pass_phrase = self.config.get('media_source_pass_phrase')
        srt_latency = self.config.get('srt_latency')
        srt_maxbw = self.config.get('srt_maxbw')
        srt_rcvbuf = self.config.get('srt_rcvbuf')


        if media_source_pass_phrase:
            self.logger.info("🔐 SRT: Encrypted connection ENABLED")
            self.logger.info("   Passphrase: [HIDDEN FOR SECURITY]")
        else:
            self.logger.info("🔓 SRT: Unencrypted connection")

        # Enhanced SRT buffering info
        if srt_latency:
            self.logger.info(f"📊 SRT BUFFERING: {srt_latency}ms latency buffer")
        else:
            self.logger.info("📊 SRT BUFFERING: Default (~120ms)")

        if srt_maxbw:
            self.logger.info(f"🌐 SRT BANDWIDTH: {srt_maxbw/1000000:.1f} Mbps limit")
        else:
            self.logger.info("🌐 SRT BANDWIDTH: Unlimited")

        if srt_rcvbuf:
            self.logger.info(f"🗄️ SRT BUFFER: {srt_rcvbuf/1024/1024:.1f}MB receive buffer")

        # Connection retry settings
        self.logger.info(f"🔄 RETRY: Max {self.max_connection_retries} attempts, {self.base_retry_delay}s-{self.max_retry_delay}s delays")
        self.logger.info("🛡️ STALL DETECTION: Advanced monitoring enabled")

        if video_process:
            video_crf = self.config.get('video_crf')
            video_bitrate = self.config.get('video_bitrate')
            video_preset = self.config.get('video_preset', 'medium')
            video_fps = self.config.get('video_fps')
            video_width = self.config.get('video_width')
            video_height = self.config.get('video_height')

            self.logger.info("📹 VIDEO: H.264 Encoding ENABLED")
            if video_crf:
                self.logger.info(f"   Quality: CRF {video_crf} (quality-based)")
            elif video_bitrate:
                self.logger.info(f"   Quality: {video_bitrate} (bitrate-based)")
            self.logger.info(f"   Preset: {video_preset}")
            if video_fps:
                self.logger.info(f"   Frame Rate: {video_fps} fps")
            if video_width and video_height:
                self.logger.info(f"   Resolution: {video_width}x{video_height}")
        else:
            self.logger.info("📹 VIDEO: Copy mode (no processing)")

        if audio_downmix:
            audio_bitrate = self.config.get('audio_bitrate', '192k')
            self.logger.info(f"🎵 AUDIO: Downmix to stereo @ {audio_bitrate}")
        else:
            self.logger.info("🎵 AUDIO: Copy mode (preserve original)")

        self.logger.info("================================")

    def test_srt_connection(self, timeout_seconds=15):
        """Test SRT connection without starting full recording"""
        self.logger.info("🔗 Testing SRT connection...")

        srt_options = self.get_srt_input_options()

        cmd = ['ffmpeg', '-y', '-v', 'error']

        # Add SRT options before input URL
        cmd.extend(srt_options)
        cmd.extend(['-i', self.config['media_source_url']])

        # Add output options
        cmd.extend([
            '-t', '3',  # Just test for 3 seconds
            '-f', 'null',  # Output to null
            '-'  # Output target
        ])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)

            if result.returncode == 0:
                self.logger.info("✅ SRT connection test successful")
                return True
            else:
                error_msg = result.stderr.lower()
                if "connection refused" in error_msg:
                    self.logger.warning("🚫 SRT connection refused - server may be unavailable")
                elif "timeout" in error_msg or "timed out" in error_msg:
                    self.logger.warning("⏰ SRT connection timeout - network issues or server slow")
                elif "authentication" in error_msg or "passphrase" in error_msg:
                    self.logger.warning("🔐 SRT authentication failed - check passphrase")
                else:
                    self.logger.warning(f"❌ SRT connection failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.warning("⏰ SRT connection test timed out")
            return False
        except Exception as e:
            self.logger.warning(f"❌ SRT connection test error: {e}")
            return False

    def start_ffmpeg_with_retry(self, segment_seconds, is_scheduled=False, duration_minutes=None):
        """Start FFmpeg with robust connection retry logic"""

        for attempt in range(self.max_connection_retries):
            attempt_num = attempt + 1
            self.logger.info(f"🔄 Starting FFmpeg connection (attempt {attempt_num}/{self.max_connection_retries})")

            # Test connection first -- need to move this elsewhere - this added extra time to the recording_start_time
            if not self.test_srt_connection():
                self.logger.warning(f"⚠️ Connection test failed on attempt {attempt_num}")
            else:
                # Connection test passed, try full FFmpeg start
                try:
                    if is_scheduled and duration_minutes:
                        process = self.start_ffmpeg_scheduled(duration_minutes, segment_seconds)
                    else:
                        process = self.start_ffmpeg_continuous(segment_seconds)

                    if process:
                        # Give FFmpeg time to establish stable connection
                        self.logger.info("⏳ Waiting for FFmpeg to stabilize...")
                        time.sleep(15)

                        if process.poll() is None:  # Still running
                            self.logger.info("✅ FFmpeg connection established successfully")
                            if attempt > 0:
                                self.stats['reconnections'] += 1
                                self.logger.info(f"🎉 Successfully reconnected after {attempt} failed attempts")
                            return process
                        else:
                            self.logger.warning(f"💥 FFmpeg process died immediately on attempt {attempt_num}")

                except Exception as e:
                    self.logger.error(f"🚨 FFmpeg start failed on attempt {attempt_num}: {e}")

            # Connection/start failed, prepare for retry
            self.stats['connection_retries'] += 1

            if attempt < self.max_connection_retries - 1:  # Not the last attempt
                # Exponential backoff with jitter
                retry_delay = min(
                    self.base_retry_delay * (2 ** attempt),  # Exponential backoff
                    self.max_retry_delay  # Cap at max delay
                )

                self.logger.info(f"⏰ Connection failed. Waiting {retry_delay}s before retry {attempt_num + 1}/{self.max_connection_retries}...")

                # Sleep in smaller chunks so we can respond to shutdown signals
                for _ in range(int(retry_delay)):
                    if not self.running:
                        self.logger.info("🛑 Shutdown requested during retry delay")
                        return None
                    time.sleep(1)
            else:
                self.logger.error(f"💔 All {self.max_connection_retries} connection attempts failed")

        return None

    def reset_processing_state_for_reconnection(self):
        """Reset processing state after reconnection to fix processing logic issues"""
        self.logger.info("🔄 RECONNECTION FIX: Resetting processing state...")

        # Store old state for logging
        old_processed_count = len(self.processed_segments)
        old_processed_list = list(self.processed_segments)

        # Clear the processed segments set - this is the main fix
        self.processed_segments.clear()

        # Log what we cleared
        self.logger.info(f"   Cleared {old_processed_count} processed segment entries:")
        for old_segment in old_processed_list[:5]:  # Show first 5
            self.logger.info(f"     • {old_segment}")
        if len(old_processed_list) > 5:
            self.logger.info(f"     • ... and {len(old_processed_list) - 5} more")

        # Process any orphaned segments that got left behind
        temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                          key=lambda x: int(x.stem.split('_')[-1]))

        if temp_files:
            self.logger.info(f"   Found {len(temp_files)} temp files after reconnection:")
            for temp_file in temp_files:
                self.logger.info(f"     • {temp_file.name} ({temp_file.stat().st_size/1024/1024:.1f}MB)")

            # Mark files as ready for processing on next cycle
            self.logger.info("   These files will be processed on next monitoring cycle")

        self.logger.info("✅ Processing state reset complete - monitoring will resume normally")

    def monitor_connection_health(self, process):
        """Monitor FFmpeg process health and return status"""
        if not process or process.poll() is not None:
            return False
        return True

    def get_srt_input_options(self):
        """Get SRT input options including enhanced buffering settings"""
        srt_options = []

        # Get SRT passphrase if configured
        media_source_pass_phrase = self.config.get('media_source_pass_phrase')
        if media_source_pass_phrase:
            # URL encode the passphrase to handle special characters
            encoded_passphrase = urllib.parse.quote(media_source_pass_phrase, safe='')
            srt_options.extend(['-passphrase', encoded_passphrase])
            self.logger.debug("SRT passphrase configured (encoded for special characters)")

        # Get optional SRT encryption key length
        srt_pbkeylen = self.config.get('srt_pbkeylen')
        if srt_pbkeylen:
            srt_options.extend(['-pbkeylen', str(srt_pbkeylen)])
            self.logger.debug(f"SRT key length set to {srt_pbkeylen} bits")

        # Enhanced SRT connection options for network stability
        srt_latency = self.config.get('srt_latency')
        if srt_latency:
            srt_options.extend(['-latency', str(srt_latency)])
            self.logger.debug(f"SRT latency set to {srt_latency}ms")

        srt_maxbw = self.config.get('srt_maxbw')
        if srt_maxbw:
            srt_options.extend(['-maxbw', str(srt_maxbw)])
            self.logger.debug(f"SRT max bandwidth set to {srt_maxbw}")

        # Additional buffer settings for network stability
        srt_rcvbuf = self.config.get('srt_rcvbuf')
        if srt_rcvbuf:
            srt_options.extend(['-rcvbuf', str(srt_rcvbuf)])
            self.logger.debug(f"SRT receive buffer set to {srt_rcvbuf} bytes")

        srt_sndbuf = self.config.get('srt_sndbuf')
        if srt_sndbuf:
            srt_options.extend(['-sndbuf', str(srt_sndbuf)])
            self.logger.debug(f"SRT send buffer set to {srt_sndbuf} bytes")

        return srt_options

    def cleanup_temp_files(self):
        """Clean up any leftover temp files from previous runs"""
        temp_files = list(self.segments_dir.glob("temp_segment_*.mp4"))
        if temp_files:
            self.logger.info(f"Cleaning up {len(temp_files)} leftover temp files")
            for temp_file in temp_files:
                temp_file.unlink()
        else:
            self.logger.info("No leftover temp files to clean up")

    def retry_failed_uploads(self):
        """Check for and retry any properly named files that failed to upload"""
        """Pattern: $MEDIA_SOURCE_ID_$SCHEDULED_JOB_ID_$EPOCHSTARTTIME_$INGESTED_SEGMENT_LENGTH_IN_SECONDS.$FMT  """
        """ 1755063115 """
        pattern = f"{self.config['media_source_id'].zfill(10)}_{self.config['scheduled_job_id'].zfill(10)}_??????????_????.mp4"
        failed_files = list(self.segments_dir.glob(pattern))

        if failed_files:
            self.logger.info(f"Found {len(failed_files)} files that may have failed upload, retrying...")
            for file_path in failed_files:
                self.logger.info(f"Retrying upload for {file_path.name}")
                self.upload_executor.submit(self.upload_to_s3, file_path)
        else:
            self.logger.info("No failed upload files found")

    def calculate_next_recording_time(self, schedule_config):
        """Calculate the next recording time based on weekly schedule"""
        now = datetime.now(timezone.utc)
        weekly_schedule = schedule_config['weekly_schedule']
        start_date = datetime.fromisoformat(schedule_config['start_date']).date()
        end_date = None
        if schedule_config.get('end_date'):
            end_date = datetime.fromisoformat(schedule_config['end_date']).date()

        # Day name mapping
        day_names = {
            0: 'monday', 1: 'tuesday', 2: 'wednesday', 3: 'thursday',
            4: 'friday', 5: 'saturday', 6: 'sunday'
        }

        # Check if we're past the end date
        if end_date and now.date() > end_date:
            return None

        # Look for next recording time (check next 14 days to handle any schedule)
        for days_ahead in range(14):
            check_date = now.date() + timedelta(days=days_ahead)

            # Skip if before start date
            if check_date < start_date:
                continue

            # Skip if past end date
            if end_date and check_date > end_date:
                continue

            # Get day name
            weekday = check_date.weekday()  # 0 = Monday
            day_name = day_names[weekday]

            # Check if this day has a schedule
            if day_name not in weekly_schedule:
                continue

            day_schedule = weekly_schedule[day_name]
            start_time = datetime.strptime(day_schedule['start_time'], '%H:%M').time()
            end_time = datetime.strptime(day_schedule['end_time'], '%H:%M').time()

            # Handle end time next day (like 24:00 or past midnight)
            recording_start = datetime.combine(check_date, start_time, timezone.utc)
            recording_end = datetime.combine(check_date, end_time, timezone.utc)

            if end_time <= start_time:  # Crosses midnight
                recording_end += timedelta(days=1)

            # If this is today, check if recording hasn't started yet
            if days_ahead == 0:
                if now < recording_start:
                    return recording_start, recording_end
                # If we're currently in a recording window, don't start a new one
                elif recording_start <= now < recording_end:
                    self.logger.info(f"Currently in recording window for {day_name}, skipping to next scheduled time")
                    continue
            else:
                # Future day, this is our next recording
                return recording_start, recording_end

        return None  # No more recordings scheduled

    def calculate_aligned_times(self, start_time, end_time, segment_minutes=5):
        """Calculate recording times aligned to 5-minute boundaries"""
        # Align start time to 5-minute boundary
        aligned_minute = (start_time.minute // segment_minutes) * segment_minutes
        aligned_start = start_time.replace(minute=aligned_minute, second=0, microsecond=0)

        # Align end time to 5-minute boundary (round up)
        end_aligned_minute = ((end_time.minute + segment_minutes - 1) // segment_minutes) * segment_minutes
        if end_aligned_minute >= 60:
            aligned_end = end_time.replace(hour=end_time.hour + 1, minute=0, second=0, microsecond=0)
        else:
            aligned_end = end_time.replace(minute=end_aligned_minute, second=0, microsecond=0)

        duration_minutes = int((aligned_end - aligned_start).total_seconds() / 60)

        return aligned_start, aligned_end, duration_minutes

    def get_next_aligned_time(self, segment_minutes=5):
        """Get the next 5-minute aligned time (for continuous mode)"""
        now = datetime.now(timezone.utc)
         # Calculate next 5-minute boundary
        next_minute = ((now.minute // segment_minutes) + 1) * segment_minutes

        if next_minute >= 60:
            next_start = now.replace(hour=now.hour + 1, minute=0, second=0, microsecond=0)
        else:
            next_start = now.replace(minute=next_minute, second=0, microsecond=0)

        return next_start

    def generate_continuous_filename(self, segment_start_time, segment_seconds):
        """Generate filename for continuous recording segments"""
        #date_str = segment_start_time.strftime("%Y%m%d")
        #time_str = segment_start_time.strftime("%H%M")
        epoch_ts=segment_start_time.timestamp()
        return f"{str(self.config['media_source_id']).zfill(10)}_{str(self.config['scheduled_job_id']).zfill(10)}_{str(int(epoch_ts)).zfill(12)}_{str(segment_seconds).zfill(4)}.mp4"

    def generate_segment_list(self, show_start, show_end, segment_minutes):
        """Generate list of expected segments for scheduled recording"""
        segments = []
        current_time = show_start

        while current_time < show_end:
            date_str = current_time.strftime("%Y%m%d")
            time_str = current_time.strftime("%H%M")
            epoch_timestamp = current_time.timestamp()
            filename = f"{self.config['media_source_id']}__{self.config['media_source_id']}__{epoch_timestamp}__{segment_minutes}.mp4"

            segments.append({
                'start_time': current_time,
                'filename': filename,
                'temp_filename': f"temp_segment_{len(segments)+1:03d}.mp4"
            })

            current_time += timedelta(minutes=segment_minutes)

        return segments

    def detect_best_audio_stream(self):
        """Auto-detect the best audio stream with informational descriptive audio labeling"""
        try:
            # Build ffprobe command with SRT options
            srt_options = self.get_srt_input_options()

            cmd = ['ffprobe', '-v', 'quiet', '-show_streams',
                   '-select_streams', 'a', '-show_entries',
                   'stream=index,channels,channel_layout,bit_rate,tags']

            # Add SRT options before the input URL
            cmd.extend(srt_options)
            cmd.append(self.config['media_source_url'])

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode != 0:
                self.logger.warning("Could not probe audio streams, using default")
                return 1

            streams = []
            current_stream = {}

            for line in result.stdout.strip().split('\n'):
                if line.startswith('[STREAM]'):
                    current_stream = {}
                elif line.startswith('[/STREAM]'):
                    if current_stream:
                        streams.append(current_stream)
                elif '=' in line:
                    key, value = line.split('=', 1)
                    current_stream[key] = value

            if not streams:
                self.logger.warning("No audio streams detected, using default")
                return 1

            # Enhanced stream analysis
            scored_streams = []

            for stream in streams:
                score = 5  # Base score for all streams
                channels = int(stream.get('channels', '0'))
                bit_rate = int(stream.get('bit_rate', '0'))
                index = int(stream.get('index', '0'))
                tags = stream.get('tags', {})
                title = tags.get('title', '').lower()

                # Detect descriptive audio for informational purposes only
                descriptive_keywords = ['description', 'descriptive', 'vi', 'accessibility', 'sap', 'secondary', 'alternate']
                is_descriptive = any(keyword in title for keyword in descriptive_keywords)

                if is_descriptive:
                    self.logger.info(f"Stream {index}: DESCRIPTIVE AUDIO detected ({title})")

                # Score all streams fairly based on quality indicators

                # 1. Channel preference: 5.1 > Stereo > Mono
                if channels >= 5:  # 5.1 or more
                    score += 50
                    self.logger.info(f"Stream {index}: +50 for 5.1+ surround ({channels} channels)")
                elif channels == 2:
                    score += 30
                    self.logger.info(f"Stream {index}: +30 for stereo")
                elif channels == 1:
                    score += 10
                    self.logger.info(f"Stream {index}: +10 for mono")

                # 2. Bitrate preference (higher = better quality)
                if bit_rate > 600000:  # > 600k (high quality)
                    score += 30
                    self.logger.info(f"Stream {index}: +30 for high bitrate ({bit_rate//1000}k)")
                elif bit_rate > 256000:  # > 256k (good quality)
                    score += 20
                    self.logger.info(f"Stream {index}: +20 for medium bitrate ({bit_rate//1000}k)")
                elif bit_rate > 128000:  # > 128k (acceptable)
                    score += 10
                    self.logger.info(f"Stream {index}: +10 for decent bitrate ({bit_rate//1000}k)")
                else:
                    score += 5
                    self.logger.info(f"Stream {index}: +5 for low bitrate ({bit_rate//1000}k)")

                # 3. Stream index preference (earlier streams often main content)
                if index <= 1:
                    score += 10
                    self.logger.info(f"Stream {index}: +10 for early stream index")

                # 4. Look for "main" indicators in metadata
                main_keywords = ['main', 'primary', 'program']
                if any(keyword in title for keyword in main_keywords):
                    score += 20
                    self.logger.info(f"Stream {index}: +20 for main audio indicator ({title})")

                scored_streams.append({
                    'index': index,
                    'score': score,
                    'channels': channels,
                    'bit_rate': bit_rate,
                    'title': title,
                    'is_descriptive': is_descriptive,
                    'stream': stream
                })

            if not scored_streams:
                self.logger.warning("No suitable audio streams found, using default")
                return 1

            # Sort by score (highest first)
            scored_streams.sort(key=lambda x: x['score'], reverse=True)

            # Always show all streams with clear information
            self.logger.info("Audio stream analysis results:")
            for s in scored_streams:
                desc_indicator = " [DESCRIPTIVE]" if s['is_descriptive'] else ""
                self.logger.info(f"  Stream {s['index']}: Score {s['score']} "
                               f"({s['channels']}ch, {s['bit_rate']//1000}k, '{s['title']}'){desc_indicator}")

            # Select the highest scored stream
            best_stream = scored_streams[0]

            # Inform user about the selection with clear messaging
            desc_note = " (descriptive audio - user can override if needed)" if best_stream['is_descriptive'] else ""
            self.logger.info(f"Auto-selected audio stream {best_stream['index']} "
                           f"({best_stream['channels']} channels, {best_stream['bit_rate']//1000}k){desc_note}")

            return best_stream['index']

        except Exception as e:
            self.logger.warning(f"Audio stream detection failed: {e}, using default")
            return 1

    def get_stream_mapping(self):
        """Get video and audio stream mapping based on config"""
        # Video stream
        video_index = self.config.get('video_stream_index', 0)

        # Audio stream
        audio_config = self.config.get('audio_stream_index', 'auto')

        if audio_config == 'auto':
            audio_index = self.detect_best_audio_stream()
        else:
            audio_index = int(audio_config)

        self.logger.info(f"Using video stream 0:{video_index}, audio stream 0:{audio_index}")
        return video_index, audio_index

    def get_audio_codec_settings(self):
        """Get audio codec settings based on config (copy vs downmix)"""
        downmix_enabled = self.config.get('audio_downmix', False)
        audio_bitrate = self.config.get('audio_bitrate', '192k')

        if downmix_enabled:
            self.logger.info(f"Audio downmixing enabled: surround → stereo at {audio_bitrate}")
            return [
                '-c:a', 'aac',          # Encode to AAC
                '-ac', '2',             # Force stereo output
                '-b:a', audio_bitrate   # Set bitrate
            ]
        else:
            self.logger.info("Audio copy mode: preserving original format")
            return ['-c:a', 'copy']     # Copy original audio

    def get_video_codec_settings(self):
        """Get video codec settings based on config (copy vs process)"""
        video_process = self.config.get('video_process', False)

        if not video_process:
            self.logger.info("Video copy mode: preserving original format")
            return ['-c:v', 'copy']

        # Video processing enabled
        target_fps = self.config.get('video_fps', 30)
        target_width = self.config.get('video_width', None)
        target_height = self.config.get('video_height', None)
        video_bitrate = self.config.get('video_bitrate', '2000k')
        video_preset = self.config.get('video_preset', 'medium')
        video_crf = self.config.get('video_crf', None)

        settings = [
            '-c:v', 'libx264',      # H.264 encoder
            '-preset', video_preset, # Encoding speed/quality preset
        ]

        # Frame rate settings
        if target_fps:
            settings.extend(['-r', str(target_fps)])
            self.logger.info(f"Video processing: target {target_fps} fps")

        # Resolution settings
        if target_width and target_height:
            settings.extend(['-s', f'{target_width}x{target_height}'])
            self.logger.info(f"Video processing: target resolution {target_width}x{target_height}")
        elif target_width or target_height:
            # Scale maintaining aspect ratio
            if target_width:
                scale_filter = f'scale={target_width}:-2'
            else:
                scale_filter = f'scale=-2:{target_height}'
            settings.extend(['-vf', scale_filter])
            self.logger.info(f"Video processing: scaling to {scale_filter}")

        # Quality settings - prefer CRF over bitrate if specified
        if video_crf:
            settings.extend(['-crf', str(video_crf)])
            self.logger.info(f"Video processing: CRF {video_crf} (quality-based)")
        else:
            settings.extend(['-b:v', video_bitrate])
            self.logger.info(f"Video processing: bitrate {video_bitrate}")

        # Additional encoding settings for TV content
        settings.extend([
            '-profile:v', 'main',       # H.264 profile (compatible)
            '-level:v', '4.0',          # H.264 level
            '-pix_fmt', 'yuv420p',      # Pixel format (universal compatibility)
            '-movflags', '+faststart'   # Optimize for streaming
        ])
        self.logger.info(f"Video processing enabled: {video_preset} preset")
        return settings

    def wait_for_time(self, wait_seconds, target_time):
        """Wait until target time"""
        if wait_seconds > 0:
            self.logger.info(f"Waiting {wait_seconds:.1f} seconds to start at {target_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            time.sleep(wait_seconds)

    def start_ffmpeg_continuous(self, segment_seconds):
        """Start FFmpeg with aggressive 5-minute segmentation - called by retry logic"""
         # Get optimal stream mapping
        video_index, audio_index = self.get_stream_mapping()

        # Get codec settings (copy or process)
        video_settings = self.get_video_codec_settings()
        audio_settings = self.get_audio_codec_settings()

        # Get SRT input options (passphrase, buffering, etc.)
        srt_options = self.get_srt_input_options()

        # AGGRESSIVE PARAMETERS - Force exact timing
        cmd = [
            'ffmpeg', '-y',

            # Input settings - Be aggressive about connection
            '-thread_queue_size', '2048',
            '-fflags', '+genpts+igndts+discardcorrupt',
            '-re',  # Real-time mode
        ]

        # Add SRT options before input URL
        cmd.extend(srt_options)
        cmd.extend(['-i', self.config['media_source_url']])

        # Stream mapping and codec settings
        cmd.extend([
            '-map', f'0:{video_index}',
            '-map', f'0:{audio_index}',

            # Codec settings (dynamic based on config)
            *video_settings,  # Video: copy or process
            *audio_settings,  # Audio: copy or downmix

            # AGGRESSIVE SEGMENTATION SETTINGS
            '-f', 'segment',
            '-segment_time', str(segment_seconds),          # Exact segment duration
            '-segment_format', 'mp4',
            '-segment_format_options', 'movflags=+faststart',
            '-reset_timestamps', '1',
            '-segment_wrap', '999999',

            # FORCE EXACT TIMING (AGGRESSIVE MODE)
            '-force_key_frames', f'expr:gte(t,n_forced*{segment_seconds})',  # Force keyframes every 5 min
            '-break_non_keyframes', '1',                    # Break on any frame if needed
            '-segment_time_delta', '0.1',                   # Minimal tolerance (0.1s)

            # Timing and sync
            '-avoid_negative_ts', 'make_zero',
            '-max_muxing_queue_size', '4096',
            '-segment_start_number', '0',

            # Output pattern
            str(self.segments_dir / 'temp_segment_%06d.mp4')
        ])

        self.logger.info(f"Starting aggressive {segment_seconds} seconds segmentation with enhanced SRT buffering")
        # Log command without sensitive information
        cmd_safe = [arg if arg != self.config.get('media_source_pass_phrase', '') else '[PASSPHRASE]' for arg in cmd]
        self.logger.info(f"Command: {' '.join(cmd_safe)}")

        self.ffmpeg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        ## Need to set the `recording_start_time` here!
        # Start a thread to monitor FFmpeg output for debugging
        threading.Thread(target=self._monitor_ffmpeg_output, daemon=True).start()

        return self.ffmpeg_process

    def start_ffmpeg_scheduled(self, duration_minutes, segment_seconds):
        """Start FFmpeg for scheduled recording - called by retry logic"""
        total_seconds = duration_minutes * 60
        # Get optimal stream mapping
        video_index, audio_index = self.get_stream_mapping()

        # Get codec settings (copy or process)
        video_settings = self.get_video_codec_settings()
        audio_settings = self.get_audio_codec_settings()

        # Get SRT input options (passphrase, buffering, etc.)
        srt_options = self.get_srt_input_options()

        # Use same aggressive approach for scheduled recording
        cmd = [
            'ffmpeg', '-y',
            '-thread_queue_size', '2048',
            '-fflags', '+genpts+igndts+discardcorrupt',
            '-re',
        ]

        # Add SRT options before input URL
        cmd.extend(srt_options)
        cmd.extend(['-i', self.config['media_source_url']])

        # Stream mapping and codec settings
        cmd.extend([
            '-map', f'0:{video_index}',
            '-map', f'0:{audio_index}',

            # Codec settings (dynamic based on config)
            *video_settings,  # Video: copy or process
            *audio_settings,  # Audio: copy or downmix

            '-f', 'segment',
            '-segment_time', str(segment_seconds),
            '-segment_format', 'mp4',
            '-segment_format_options', 'movflags=+faststart',
            '-reset_timestamps', '1',
            '-force_key_frames', f'expr:gte(t,n_forced*{segment_seconds})',
            '-break_non_keyframes', '1',
            '-segment_time_delta', '0.1',
            '-avoid_negative_ts', 'make_zero',
            '-max_muxing_queue_size', '4096',
            '-segment_start_number', '0',
            '-t', str(total_seconds),
            str(self.segments_dir / 'temp_segment_%03d.mp4')
        ])

        self.logger.info(f"Starting FFmpeg for SCHEDULED recording: {duration_minutes} minutes with {segment_seconds} seconds segments")

        # Log command without sensitive information
        cmd_safe = [arg if arg != self.config.get('media_source_pass_phrase', '') else '[PASSPHRASE]' for arg in cmd]
        self.logger.info(f"Command: {' '.join(cmd_safe)}")

        self.ffmpeg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        return self.ffmpeg_process

    def _monitor_ffmpeg_output(self):
        """Monitor FFmpeg stderr with realistic performance thresholds and save everything to rotating file"""
        if not self.ffmpeg_process:
            return

        self.logger.info("Starting FFmpeg monitoring with enhanced network stability detection")

        # Create rotating FFmpeg log file for full troubleshooting
        ffmpeg_log_file = self.segments_dir / 'ffmpeg_debug.log'

        # Manage FFmpeg log file size (rotate when > 50MB)
        if ffmpeg_log_file.exists() and ffmpeg_log_file.stat().st_size > 50 * 1024 * 1024:
            # Rotate: keep only the last 2 files
            old_log = self.segments_dir / 'ffmpeg_debug.log.1'
            if old_log.exists():
                old_log.unlink()  # Delete oldest
            ffmpeg_log_file.rename(old_log)  # Current becomes .1

        # Track repetitive errors and performance
        error_counts = {}
        last_speed_log = 0
        last_progress_log = 0

        # Get configurable performance threshold (default 0.85, more lenient for buffered streams)
        perf_warning_threshold = self.config.get('performance_warning_threshold', 0.75)

        try:
            with open(ffmpeg_log_file, 'a') as debug_log:  # Append mode for rotation
                debug_log.write(f"\n=== FFmpeg V3 Session Started: {datetime.now(timezone.utc).isoformat()} ===\n")
                debug_log.flush()

                for line in iter(self.ffmpeg_process.stderr.readline, ''):
                    if not self.running:
                        break

                    line = line.strip()
                    if line:
                        # ALWAYS save everything to debug log file
                        debug_log.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d  %H:%M:%S')}: {line}\n")
                        debug_log.flush()

                        # Enhanced network error detection
                        if 'RCV-DROPPED' in line:
                            self.logger.warning(f"📦 SRT packet loss detected: {line}")
                            continue

                        if 'delayed for' in line and 'ms' in line:
                            self.logger.warning(f"⏰ SRT packet delay detected: {line}")
                            continue

                        if 'Packet corrupt' in line or 'PES packet size mismatch' in line:
                            self.logger.warning(f"🔧 SRT stream corruption: {line}")
                            continue

                        # Smart filtering for main log
                        if 'decode_slice_header error' in line:
                            error_counts['decode_slice_header'] = error_counts.get('decode_slice_header', 0) + 1
                            if error_counts['decode_slice_header'] <= 2:
                                self.logger.debug(f"FFmpeg: {line}")
                            elif error_counts['decode_slice_header'] == 10:
                                self.logger.info("FFmpeg: Connection established (initial decode errors normal)")
                            continue

                        # Always log real errors and warnings
                        if any(keyword in line.lower() for keyword in ['error', 'warning']) and 'decode_slice_header' not in line:
                            self.logger.warning(f"FFmpeg: {line}")
                            continue

                        # Log segment creation
                        if 'segment' in line.lower():
                            self.logger.info(f"FFmpeg: {line}")
                            continue

                        # Performance monitoring with more lenient thresholds for buffered streams
                        if 'speed=' in line:
                            import time
                            current_time = time.time()

                            # Extract speed value
                            try:
                                speed_part = [part for part in line.split() if 'speed=' in part][0]
                                speed_value = float(speed_part.replace('speed=', '').replace('x', ''))

                                # More lenient performance thresholds for buffered streams
                                if speed_value < perf_warning_threshold:  # Default 0.75 for buffered
                                    self.logger.warning(f"FFmpeg performance issue: {speed_part} (target: ≥{perf_warning_threshold}x)")
                                    last_speed_log = current_time
                                # Log good performance periodically (every 5 minutes)
                                elif current_time - last_speed_log > 300:  # 5 minutes
                                    self.logger.info(f"FFmpeg performance: {speed_part} ✓")
                                    last_speed_log = current_time

                            except (ValueError, IndexError):
                                pass  # Skip if we can't parse speed

                        # Very occasional progress updates (every 2-3 minutes)
                        elif 'time=' in line and 'fps=' in line:
                            import time
                            current_time = time.time()
                            if current_time - last_progress_log > 150:  # 2.5 minutes
                                # Only log a brief progress summary
                                try:
                                    time_part = [part for part in line.split() if 'time=' in part][0]
                                    fps_part = [part for part in line.split() if 'fps=' in part][0]
                                    self.logger.info(f"FFmpeg status: {time_part} {fps_part}")
                                    last_progress_log = current_time
                                except IndexError:
                                    pass

        except Exception as e:
            self.logger.error(f"Error monitoring FFmpeg output: {e}")

        # Log where to find the full debug info
        self.logger.info(f"Full FFmpeg output saved to: {ffmpeg_log_file} (auto-rotated at 50MB)")

    def monitor_continuous_segments_with_reconnect(self, segment_seconds, recording_start_time):
        """Monitor segments with automatic reconnection AND advanced stall detection"""
        self.logger.info("Starting segment monitoring with connection monitoring and advanced stall detection...")

        last_segment_count = 0
        last_segment_change_time = time.time()
        stall_timeout = segment_seconds + 60  # Segment duration + 1 minute grace period

        while self.running:
            current_time = time.time()

            # Check if FFmpeg process is still alive
            if not self.monitor_connection_health(self.ffmpeg_process):
                self.logger.error("💥 FFmpeg process died - attempting reconnection...")

                # Process any remaining segments before reconnecting
                temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                                  key=lambda x: int(x.stem.split('_')[-1]))
                self.process_final_segments(temp_files, recording_start_time, segment_seconds)

                # CRITICAL FIX: Reset processing state for reconnection
                self.reset_processing_state_for_reconnection()

                # Attempt reconnection
                self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_seconds)

                if not self.ffmpeg_process:
                    self.logger.error("💔 Could not reconnect - ending recording")
                    break
                else:
                    # Restart FFmpeg output monitoring
                    threading.Thread(target=self._monitor_ffmpeg_output, daemon=True).start()
                    self.logger.info("🎉 Reconnected successfully - resuming segment monitoring")
                    last_segment_change_time = current_time  # Reset stall timer
                    continue

            # Regular segment monitoring
            temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                              key=lambda x: int(x.stem.split('_')[-1]))

            current_segment_count = len(temp_files)

            # Log segment detection
            if current_segment_count != last_segment_count:
                self.logger.info(f"Segment count changed: {last_segment_count} → {current_segment_count}")
                self.logger.info(f"   Files: {[f.name for f in temp_files]}")
                last_segment_count = current_segment_count
                last_segment_change_time = current_time  # Reset stall timer

            # NEW: Advanced stall detection
            time_since_last_change = current_time - last_segment_change_time

            if time_since_last_change > stall_timeout:
                self.logger.warning(f"🐌 STALL DETECTED: No new segments for {time_since_last_change:.1f}s (timeout: {stall_timeout}s)")

                # Check if we have a single stalled temp file
                if len(temp_files) == 1:
                    stalled_file = temp_files[0]
                    file_age = current_time - stalled_file.stat().st_mtime
                    file_size = stalled_file.stat().st_size

                    self.logger.warning(f"   Stalled file: {stalled_file.name} (age: {file_age:.1f}s, size: {file_size/1024/1024:.1f}MB)")

                    # If file is old and reasonable size, process it and force reconnection
                    if file_age > segment_seconds and file_size > 1024 * 1024:  # 1MB minimum
                        self.logger.info("🚨 Processing stalled segment and forcing reconnection...")

                        # Process the stalled segment
                        self.process_completed_segment(stalled_file, recording_start_time, segment_seconds)

                        # Force reconnection to restart segmentation
                        self.logger.info("🔄 Forcing reconnection to restart segment creation...")
                        if self.ffmpeg_process:
                            self.ffmpeg_process.terminate()
                            try:
                                self.ffmpeg_process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                self.ffmpeg_process.kill()

                        # CRITICAL FIX: Reset processing state for stall recovery too
                        self.reset_processing_state_for_reconnection()

                        # Reconnect
                        self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_seconds)

                        if not self.ffmpeg_process:
                            self.logger.error("💔 Could not reconnect after stall - ending recording")
                            break
                        else:
                            threading.Thread(target=self._monitor_ffmpeg_output, daemon=True).start()
                            self.logger.info("🎉 Reconnected after stall - resuming segment monitoring")
                            self.stats['stall_recoveries'] += 1
                            last_segment_change_time = current_time
                            continue

                # Reset timer to avoid spam (give it another chance)
                last_segment_change_time = current_time - (stall_timeout * 0.8)  # Wait 80% of timeout before checking again

            # Process completed segments (existing logic)
            if len(temp_files) >= 2:
                # Process all files except the last one (which is still being written)
                for temp_file in temp_files[:-1]:  # All except the last (current) file
                    if temp_file.name not in self.processed_segments:
                        self.logger.debug(f"🔍 Checking if {temp_file.name} is ready for processing...")
                        if self.is_segment_complete_and_ready(temp_file):
                            self.logger.info(f"✅ Processing {temp_file.name} (ready and not in processed set)")
                            self.process_completed_segment(temp_file, recording_start_time, segment_seconds)
                        else:
                            self.logger.debug(f"⏳ {temp_file.name} not ready yet, waiting...")
                    else:
                        self.logger.debug(f"⏭️ Skipping {temp_file.name} (already processed)")

            time.sleep(10)  # Check every 10 seconds

        # Process final segments
        if 'temp_files' in locals():
            self.process_final_segments(temp_files, recording_start_time, segment_seconds)

    def process_completed_segment(self, temp_file, recording_start_time, segment_seconds):
        """Process a segment that FFmpeg has finished writing - ENHANCED with duplicate protection and reconnection fixes"""
        try:
            # Calculate segment time
            segment_time = self.calculate_segment_time(temp_file.name, recording_start_time, segment_seconds)
            final_filename = self.generate_continuous_filename(segment_time, segment_seconds)
            final_path = self.segments_dir / final_filename

            self.logger.info(f"🔄 Processing segment: {temp_file.name}")
            self.logger.info(f"   Target: {final_filename}")
            self.logger.info(f"   Segment time: {segment_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            self.logger.info(f"   File size: {temp_file.stat().st_size / 1024 / 1024:.1f}MB")

            # Check if already processed (avoid double processing)
            if final_path.exists():
                self.logger.warning(f"⚠️ Final file {final_filename} already exists - skipping duplicate processing")
                temp_file.unlink()  # Remove temp file
                self.processed_segments.add(temp_file.name)
                return

            # Check if temp file still exists (might have been processed by another thread)
            if not temp_file.exists():
                self.logger.warning(f"⚠️ Temp file {temp_file.name} no longer exists - already processed?")
                self.processed_segments.add(temp_file.name)
                return

            # Rename the completed segment
            temp_file.rename(final_path)

            self.logger.info(f"✅ Segment complete: {temp_file.name} → {final_filename}")

            # Validate segment duration
            self.validate_segment_duration(final_path, segment_seconds)

            # IMPORTANT: Add to processed set BEFORE upload to prevent double processing
            self.processed_segments.add(temp_file.name)
            self.stats['segments_created'] += 1

            # Log current processing state
            self.logger.info(f"   Processed segments tracking: {len(self.processed_segments)} entries")

            # Prevent memory leak: Clean up old processed segments tracking
            if len(self.processed_segments) > 100:
                sorted_segments = sorted(self.processed_segments)
                for old_segment in sorted_segments[:50]:
                    self.processed_segments.discard(old_segment)
                self.logger.debug(f"   Cleaned up processed segments tracking: {len(self.processed_segments)} remaining")

            # Upload asynchronously
            self.upload_executor.submit(self.upload_to_s3, final_path)

        except Exception as e:
            self.logger.error(f"❌ Error processing completed segment {temp_file.name}: {e}")
            self.logger.error(f"   Exception type: {type(e).__name__}")
            # Don't mark as processed if there was an error, so it can be retried

    def validate_segment_duration(self, file_path, expected_seconds):
        """Validate that the segment has the expected duration"""
        try:
            result = subprocess.run([
                'ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
                '-of', 'csv=p=0', str(file_path)
            ], capture_output=True, text=True, timeout=10)

            if result.returncode == 0 and result.stdout.strip():
                duration = float(result.stdout.strip())

                self.logger.info(f"   Duration validation: {duration:.1f}s (expected: {expected_seconds}s)")

                if abs(duration - expected_seconds) > 10:  # Allow 10 second tolerance
                    self.logger.warning(f"   Duration deviation: {duration - expected_seconds:.1f}s")
                else:
                    self.logger.info(f"   Duration validation PASSED")
            else:
                self.logger.warning(f"   Could not validate duration for {file_path.name}")

        except Exception as e:
            self.logger.warning(f"Duration validation failed for {file_path.name}: {e}")

    def process_final_segments(self, temp_files, recording_start_time, segment_seconds):
        """Process any remaining segments after recording stops - ENHANCED"""
        self.logger.info("Processing final segments...")

        time.sleep(5)  # Wait for FFmpeg to finish writing

        for temp_file in temp_files:
            if temp_file.exists() and temp_file.name not in self.processed_segments:
                file_size = temp_file.stat().st_size

                if file_size > 100000:  # At least 100KB
                    self.logger.info(f"Processing final segment: {temp_file.name} (size: {file_size/1024/1024:.1f}MB)")
                    self.process_completed_segment(temp_file, recording_start_time, segment_seconds)
                else:
                    self.logger.warning(f"Skipping tiny final segment: {temp_file.name} (size: {file_size} bytes)")
                    temp_file.unlink()  # Clean up tiny files

    def is_segment_complete_and_ready(self, file_path):
        """Quick check if segment is ready for processing - ENHANCED with better logging"""
        try:
            if not file_path.exists():
                self.logger.debug(f"   {file_path.name}: File no longer exists")
                return False

            stat = file_path.stat()

            # Basic size check
            if stat.st_size < 100000:  # Less than 100KB is definitely incomplete
                self.logger.debug(f"   {file_path.name}: Too small ({stat.st_size} bytes)")
                return False

            # Quick stability check (file should be stable since FFmpeg moved on)
            size1 = stat.st_size
            time.sleep(1)  # Reduced from 2 seconds for faster processing

            if not file_path.exists():
                self.logger.debug(f"   {file_path.name}: File disappeared during check")
                return False

            size2 = file_path.stat().st_size

            if size1 != size2:
                self.logger.debug(f"   {file_path.name}: Still changing size ({size1} → {size2})")
                return False

            # File age check - should be at least 30 seconds old to ensure FFmpeg moved on
            file_age = time.time() - stat.st_mtime
            if file_age < 30:
                self.logger.debug(f"   {file_path.name}: Too recent (age: {file_age:.1f}s)")
                return False

            self.logger.debug(f"   {file_path.name}: Ready! (size: {size2/1024/1024:.1f}MB, age: {file_age:.1f}s)")
            return True

        except Exception as e:
            self.logger.error(f"   {file_path.name}: Error checking readiness: {e}")
            return False

    def monitor_scheduled_segments(self, expected_segments):
        """Monitor segments for scheduled recording"""
        while self.running and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
            temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                              key=lambda x: int(x.stem.split('_')[-1]))

            if len(temp_files) < 2:
                time.sleep(5)
                continue

            for temp_file in temp_files[:-1]:
                if temp_file.name not in self.processed_segments:
                    segment_info = self.match_temp_to_expected(temp_file.name, expected_segments)

                    if segment_info and self.is_segment_complete_and_ready(temp_file):
                        final_path = self.segments_dir / segment_info['filename']
                        temp_file.rename(final_path)

                        self.logger.info(f"Completed segment: {temp_file.name} → {segment_info['filename']}")
                        self.logger.info(f"  Segment time: {segment_info['start_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
                        self.logger.info(f"  File size: {final_path.stat().st_size / 1024 / 1024:.1f}MB")

                        # Validate duration
                        self.validate_segment_duration(final_path, 300)

                        self.processed_segments.add(temp_file.name)
                        self.stats['segments_created'] += 1

                        self.upload_executor.submit(self.upload_to_s3, final_path)

            time.sleep(10)

        time.sleep(5)
        self.process_remaining_scheduled_segments(expected_segments)

    def calculate_segment_time(self, temp_filename, recording_start_time, segment_seconds):
        """Calculate segment start time for continuous recording"""
        try:
            """ checking to see if segments_dir already in the name """
            if not temp_filename.startswith(self.segments_dir.name):
                realfilename = str(self.segments_dir / temp_filename)
                temp_filename = realfilename
            """ should use the file creation time """
            stat_info = os.stat(temp_filename)
            try:
                creation_timestamp = stat_info.st_birthtime
            except AttributeError:
                # Fallback for systems without st_birthtime (e.g., older Linux kernels)
                # Also the modified time would be when the file is closed, after ingesting for {segments_seconds} seconds
                # So subtract the segment_seconds --
                # TODO: note that this may not be true for the last segment!
                creation_timestamp = stat_info.st_mtime-segment_seconds

            creation_time =  datetime.fromtimestamp(creation_timestamp)
            self.logger.info(f"{temp_filename} segment start time: {creation_time}, epoch={creation_timestamp}, recording_start_time={recording_start_time}")

            #seq_num = int(temp_filename.replace('temp_segment_', '').replace('.mp4', ''))
            #segment_offset_seconds = seq_num * segment_seconds
            #return recording_start_time + timedelta(seconds=segment_offset_seconds)
            return creation_time
        except:
            self.logger.warning(f"Calculating segment start time: {temp_filename} Got exception: {sys.exc_info()}")
            return self.get_next_aligned_time(segment_seconds)

    def match_temp_to_expected(self, temp_filename, expected_segments):
        """Match temp file to expected segment (for scheduled recording)"""
        try:
            seq_num = int(temp_filename.replace('temp_segment_', '').replace('.mp4', ''))
            if 1 <= seq_num <= len(expected_segments):
                return expected_segments[seq_num - 1]
        except:
            pass
        return None

    def process_remaining_scheduled_segments(self, expected_segments):
        """Process remaining segments for scheduled recording"""
        self.logger.info("Processing remaining scheduled segments...")

        for temp_file in self.segments_dir.glob("temp_segment_*.mp4"):
            segment_info = self.match_temp_to_expected(temp_file.name, expected_segments)

            if segment_info and temp_file.stat().st_size > 1000:
                final_path = self.segments_dir / segment_info['filename']
                temp_file.rename(final_path)

                self.logger.info(f"Final: {temp_file.name} → {segment_info['filename']}")
                self.stats['segments_created'] += 1

                self.upload_to_s3(final_path)

    def upload_to_s3(self, file_path):
        """Upload file to S3 with retry logic"""
        for attempt in range(3):
            try:
                """ bucket_path/filepath.name """
                s3_key = f"{self.config['bucket_path']}/{file_path.name}"

                self.logger.info(f"Uploading {file_path.name} to {s3_key} (attempt {attempt + 1})")

                self.s3.upload_file(
                    str(file_path),
                    self.config['bucket'],
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'video/mp4',
                        'Metadata': {
                            'media_source_id': str(self.config['media_source_id']),
                            'scheduled_job_id': str(self.config['scheduled_job_id']),
                            'upload_time': datetime.now(timezone.utc).isoformat(),
                            'recording_mode': self.recording_mode,
                            'recorder_version': 'v3'
                        }
                    }
                )

                self.logger.info(f"Successfully uploaded {file_path.name}")
                file_path.unlink()  # Delete local file after successful upload
                self.stats['segments_uploaded'] += 1
                return True

            except Exception as e:
                self.logger.error(f"Upload attempt {attempt + 1} failed for {file_path.name}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))

        self.logger.error(f"All upload attempts failed for {file_path.name}")
        self.stats['upload_failures'] += 1
        return False

    """ shutdown should store some stats just in case we have files to upload, e.g. dump stats into local_segments_dir """
    """ and read back upon recover """
    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        self.logger.info(f"Shutting down V3 recorder (signal {signum})...")
        self.running = False

        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()

        self.upload_executor.shutdown(wait=True)
        if self.stats['process_start_time']:
            self.logger.info(f"Process started at { self.stats['process_start_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
        if self.stats['recording_start_time']:
            self.logger.info(f"Recording since { self.stats['recording_start_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC.")
            duration_minutes = None
            duration_hours = None
            duration_days = None

            recording_start_time_epoch  = self.stats['recording_start_time'].timestamp()
            duration = datetime.now(timezone.utc).timestamp()  - recording_start_time_epoch
            if duration > 60:
              duration_minutes = duration/60
              if duration_minutes > 60:
                duration_hours = duration_minutes / 60
                if duration_hours > 24:
                  duration_days = duration_hours/24

            self.logger.info(f"Length of recording: {duration} seconds")
            if duration_minutes is not None:
              self.logger.info(f"        {duration_minutes} minutes")
            if duration_hours is not None:
              self.logger.info(f"        {duration_hours} hours")
            if duration_days is not None:
              self.logger.info(f"        {duration_days} days")

        self.logger.info("=== Final Statistics V3 ===")
        self.logger.info(f"Segments created: {self.stats['segments_created']}")
        self.logger.info(f"Segments uploaded: {self.stats['segments_uploaded']}")
        self.logger.info(f"Upload failures: {self.stats['upload_failures']}")
        self.logger.info(f"Connection retries: {self.stats['connection_retries']}")
        self.logger.info(f"Successful reconnections: {self.stats['reconnections']}")
        self.logger.info(f"Stall recoveries: {self.stats['stall_recoveries']}")
        self.logger.info("Shutdown complete")
        sys.exit(0)

    def periodic_cleanup(self):
        """Perform periodic cleanup for long-running 24/7 operation"""
        try:
            # 1. Clean up any stray temp files that might have been missed
            temp_files = list(self.segments_dir.glob("temp_segment_*.mp4"))
            stale_temp_files = []

            for temp_file in temp_files:
                # Check if temp file is older than 1 hour (probably stale)
                file_age = time.time() - temp_file.stat().st_mtime
                if file_age > 3600:  # 1 hour
                    stale_temp_files.append(temp_file)

            if stale_temp_files:
                self.logger.warning(f"Cleaning up {len(stale_temp_files)} stale temp files older than 1 hour")
                for temp_file in stale_temp_files:
                    try:
                        temp_file.unlink()
                        self.logger.info(f"Removed stale temp file: {temp_file.name}")
                    except Exception as e:
                        self.logger.error(f"Failed to remove stale temp file {temp_file.name}: {e}")

            # 2. Log current memory usage of processed_segments for monitoring
            if len(self.processed_segments) > 50:
                self.logger.debug(f"Processed segments tracking: {len(self.processed_segments)} entries")

            # 3. Log disk space in segments directory
            try:
                total, used, free = shutil.disk_usage(self.segments_dir)
                free_gb = free // (1024**3)
                used_percent = (used / total) * 100

                if used_percent > 90:
                    self.logger.warning(f"Disk usage HIGH: {used_percent:.1f}% used, {free_gb}GB free")
                elif used_percent > 80:
                    self.logger.warning(f"Disk usage: {used_percent:.1f}% used, {free_gb}GB free")
                else:
                    self.logger.debug(f"Disk usage: {used_percent:.1f}% used, {free_gb}GB free")

            except Exception as e:
                self.logger.debug(f"Could not check disk usage: {e}")

        except Exception as e:
            self.logger.error(f"Error during periodic cleanup: {e}")

    def force_process_backlog(self):
        """Emergency method to force process any backlogged temp files"""
        self.logger.info("🚨 EMERGENCY: Force processing backlogged temp files...")

        # Clear processed segments to allow reprocessing
        self.processed_segments.clear()
        self.logger.info("   Cleared processed_segments tracking")

        # Get all temp files
        temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                          key=lambda x: int(x.stem.split('_')[-1]))

        if not temp_files:
            self.logger.info("   No temp files found - nothing to process")
            return

        self.logger.info(f"   Found {len(temp_files)} temp files to process")

        # Process all but the last file (which might still be being written)
        files_to_process = temp_files[:-1] if len(temp_files) > 1 else []

        for temp_file in files_to_process:
            try:
                # Force process without readiness checks
                recording_start_time = self.stats.get('recording_start_time') or datetime.now(timezone.utc)
                self.logger.info(f"   Force processing: {temp_file.name}")
                self.process_completed_segment(temp_file, recording_start_time, 5)
            except Exception as e:
                self.logger.error(f"   Failed to process {temp_file.name}: {e}")

        self.logger.info("✅ Emergency backlog processing complete")

    def record_continuous(self):
        """Record continuously until stopped with robust reconnection and stall detection"""
        segment_seconds = self.config['segment_length_in_seconds']
        if segment_seconds <= 0:
            segment_seconds = 300

        self.recording_mode = "continuous"
        self.logger.info(f"Starting continuous recording V3 with {segment_seconds}-seconds segments and advanced stall detection")

        # Wait for next 5-minute boundary -- should use the start time..
        # Should take from start_date_time if there
        if self.config.get('start_date_time') is not None:
          recording_start_time = datetime.utcfromtimestamp(self.config['start_date_time'])
          recording_start_time_epoch = recording_start_time.timestamp()
          current_time_epoch = datetime.now(timezone.utc).timestamp()
          # 15 seconds overhead before the realtime -- therefore the wait should end 15 seconds prior
          if recording_start_time_epoch > (current_time_epoch+15):
            wait_seconds = recording_start_time_epoch - (current_time_epoch+15)
            self.wait_for_time(wait_seconds, recording_start_time)
        else:
            recording_start_time = datetime.now(timezone.utc)
        #recording_start_time = self.get_next_aligned_time(segment_minutes)

        self.stats['recording_start_time'] = recording_start_time

        self.logger.info(f"Continuous recording started at: {recording_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        self.logger.info("Recording will continue until stopped (Ctrl+C) with automatic reconnection and stall recovery")

        # Start FFmpeg with retry logic
        self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_seconds)

        if not self.ffmpeg_process:
            self.logger.error("💔 Could not establish initial connection - exiting")
            return

        # Start monitoring thread with advanced reconnection support
        monitor_thread = threading.Thread(
            target=self.monitor_continuous_segments_with_reconnect,
            args=(segment_seconds, recording_start_time),
            daemon=True
        )
        monitor_thread.start()

        # Main loop with periodic cleanup for 24/7 operation
        try:
            second_counter = 0
            cleanup_counter = 0
            sleep_time = 60
            while self.running:
                time.sleep(sleep_time)  # Check every minute
                second_counter += sleep_time
                cleanup_counter += 1

                # Log stats every 5 minutes (should coincide with segments)
                if second_counter % segment_seconds == 0:
                    self.logger.info(f"{second_counter} seconds elapsed")
                    self.logger.info(f"Recording stats: {self.stats['segments_created']} segments created, "
                                   f"{self.stats['segments_uploaded']} uploaded, "
                                   f"{self.stats['reconnections']} reconnections, "
                                   f"{self.stats['stall_recoveries']} stall recoveries")

                    # DIAGNOSTIC: Report current temp file status
                    temp_files = sorted(self.segments_dir.glob("temp_segment_*.mp4"),
                                      key=lambda x: int(x.stem.split('_')[-1]))
                    if temp_files:
                        self.logger.info(f"📊 Current temp files: {len(temp_files)} files")
                        self.logger.info(f"   Range: {temp_files[0].name} to {temp_files[-1].name}")
                        self.logger.info(f"   Processed segments tracking: {len(self.processed_segments)} entries")

                        # Check for processing backlog
                        if len(temp_files) > 5:
                            self.logger.warning(f"⚠️ Processing backlog detected: {len(temp_files)} temp files")
                            self.logger.warning("   This may indicate processing logic issues")
                    else:
                        self.logger.info("📊 No temp files (all segments processed)")

                # Periodic cleanup every hour for 24/7 stability
                if cleanup_counter >= 60:  # Every 60 minutes
                    self.logger.info("Performing periodic 24/7 maintenance cleanup...")
                    self.periodic_cleanup()
                    cleanup_counter = 0

        except KeyboardInterrupt:
            self.logger.info("Continuous recording stopped by user")

        self.running = False
        monitor_thread.join(timeout=30)

    # tbd
    def record_scheduled(self, schedule_config_file, segment_minutes=5):
        """Record based on weekly schedule configuration with enhanced reconnection support"""
        self.recording_mode = "scheduled"

        # Load schedule configuration
        try:
            with open(schedule_config_file) as f:
                config = json.load(f)
                schedule_config = config['schedule']
        except Exception as e:
            self.logger.error(f"Failed to load schedule config {schedule_config_file}: {e}")
            return

        self.logger.info(f"Starting WEEKLY SCHEDULED recording V3 with enhanced reconnection: {schedule_config['name']}")
        self.logger.info(f"Schedule active from {schedule_config['start_date']} to {schedule_config.get('end_date', 'forever')}")

        # Log the weekly schedule
        self.logger.info("Weekly schedule:")
        for day, times in schedule_config['weekly_schedule'].items():
            self.logger.info(f"  {day.capitalize()}: {times['start_time']} - {times['end_time']}")

        # Main scheduling loop
        while self.running:
            try:
                # Calculate next recording time
                next_recording = self.calculate_next_recording_time(schedule_config)

                if next_recording is None:
                    self.logger.info("No more recordings scheduled (past end date)")
                    break

                recording_start, recording_end = next_recording

                # Get day name for logging
                day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                day_name = day_names[recording_start.weekday()]

                self.logger.info(f"Next recording: {day_name.capitalize()} {recording_start.strftime('%Y-%m-%d %H:%M:%S')} - {recording_end.strftime('%Y-%m-%d %H:%M:%S')} UTC")

                # Wait for recording time
                self.wait_for_time(0, recording_start)

                if not self.running:
                    break

                # Calculate aligned times and duration
                aligned_start, aligned_end, duration = self.calculate_aligned_times(
                    recording_start, recording_end, segment_minutes
                )

                self.logger.info(f"Starting scheduled recording:")
                self.logger.info(f"  Original: {recording_start.strftime('%H:%M:%S')} - {recording_end.strftime('%H:%M:%S')}")
                self.logger.info(f"  Aligned:  {aligned_start.strftime('%H:%M:%S')} - {aligned_end.strftime('%H:%M:%S')}")
                self.logger.info(f"  Duration: {duration} minutes")

                # Generate expected segments
                expected_segments = self.generate_segment_list(aligned_start, aligned_end, segment_minutes)

                self.logger.info("Expected segments:")
                for i, segment in enumerate(expected_segments, 1):
                    self.logger.info(f"  {i}: {segment['filename']} ({segment['start_time'].strftime('%Y-%m-%d %H:%M')})")

                self.stats['recording_start_time'] = aligned_start

                # Start recording with retry logic
                self.ffmpeg_process = self.start_ffmpeg_with_retry(segment_minutes*60, is_scheduled=True, duration_minutes=duration)

                if not self.ffmpeg_process:
                    self.logger.error("💔 Could not establish connection for scheduled recording - skipping this session")
                    continue

                # Start monitoring thread
                monitor_thread = threading.Thread(
                    target=self.monitor_scheduled_segments,
                    args=(expected_segments,),
                    daemon=True
                )
                monitor_thread.start()

                # Wait for recording completion
                try:
                    self.ffmpeg_process.wait()
                    self.logger.info(f"Scheduled recording completed: {schedule_config['name']}")
                except KeyboardInterrupt:
                    self.logger.info("Scheduled recording interrupted by user")
                    self.running = False
                    break

                monitor_thread.join(timeout=30)

                # Log completion stats
                self.logger.info(f"Recording complete - Segments created: {self.stats['segments_created']}, "
                               f"uploaded: {self.stats['segments_uploaded']}, "
                               f"stall recoveries: {self.stats['stall_recoveries']}")

                # Reset stats for next recording
                self.stats['segments_created'] = 0
                self.stats['segments_uploaded'] = 0
                self.stats['upload_failures'] = 0
                self.stats['stall_recoveries'] = 0

                # Brief pause before calculating next recording
                time.sleep(60)

            except KeyboardInterrupt:
                self.logger.info("Weekly schedule stopped by user")
                self.running = False
                break
            except Exception as e:
                self.logger.error(f"Error in scheduled recording: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

        self.logger.info("Weekly scheduled recording ended")


def main():
    parser = argparse.ArgumentParser(description='Unified Recorder V3 - Enhanced with Stall Detection, Network Stability & K8s Environment Variables')

    subparsers = parser.add_subparsers(dest='mode', help='Recording mode')

    # Continuous mode
    continuous_parser = subparsers.add_parser('continuous', help='Record continuously until stopped')
    continuous_parser.add_argument('--segment-duration', type=int, default=5,
                                 help='Segment duration in seconds (default: 300)')
    continuous_parser.add_argument('--config', default='test_config.json', help='Config file path')

    # Scheduled mode
    scheduled_parser = subparsers.add_parser('scheduled', help='Record with weekly schedule')
    scheduled_parser.add_argument('schedule_config', help='Schedule configuration file (e.g., schedules.json)')
    scheduled_parser.add_argument('--segment-duration', type=int, default=5,
                                help='Segment duration in minutes (default: 5)')
    scheduled_parser.add_argument('--config', default='test_config.json', help='Config file path')

    args = parser.parse_args()

    if not args.mode:
        parser.print_help()
        print("\nExamples:")
        print("  Continuous:  python3 unified_recording.py continuous --config test_config.json")
        print("  Scheduled:   python3 unified_recording.py scheduled schedules.json --config test_config.json")
        print("\nK8s Environment Variables:")
        print("  Set media_source_url, MEDIA_SOURCE_ID, bucket, etc. to override config file")
        sys.exit(1)

    try:
        recorder = UnifiedSRTRecorderV3(args.config)

        if args.mode == 'continuous':
            recorder.record_continuous()
        elif args.mode == 'scheduled':
            # recorder.record_scheduled(args.schedule_config, args.segment_duration)
            print("TBD")

    except Exception as e:
        stack_trace_string = traceback.format_exc()
        print(stack_trace_string)
        logging.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
