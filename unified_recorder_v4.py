#!/usr/bin/env python3
"""
Unified SRT Recorder - Supports both continuous and scheduled recording modes
"""

import subprocess
import time
import json
import boto3
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
import signal
import sys
import logging
import logging.handlers
import argparse
import psutil
from concurrent.futures import ThreadPoolExecutor
import gc
import os

class UnifiedSRTRecorder:
    def __init__(self, config_file='config.json'):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        with open(config_file) as f:
            self.config = json.load(f)
        
        # AWS clients
        self.s3 = boto3.client('s3', region_name=self.config['aws_region'])
        
        # File system
        self.segments_dir = Path(self.config['local_segments_dir'])
        self.segments_dir.mkdir(exist_ok=True, parents=True)
        
        # Resource management
        self.upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="S3Upload")
        self.processed_segments = set()
        
        # Process management
        self.running = True
        self.ffmpeg_process = None
        self.start_time = datetime.now(timezone.utc)
        self.recording_mode = None
        
        # Statistics
        self.stats = {
            'segments_created': 0,
            'segments_uploaded': 0,
            'upload_failures': 0,
            'recording_start_time': None
        }
        
        # Signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)
        
        self.logger.info("Unified SRT Recorder initialized")
    
    def setup_logging(self):
        """Setup rotating log files"""
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            'srt_recorder.log',
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
    
    def calculate_aligned_times(self, start_time_str, end_time_str, segment_minutes=5):
        """Calculate recording times aligned to 5-minute boundaries (for scheduled mode)"""
        now = datetime.now(timezone.utc)
        
        # Parse start and end times for today
        start_time = datetime.strptime(start_time_str, '%H:%M').time()
        end_time = datetime.strptime(end_time_str, '%H:%M').time()
        
        # Create datetime objects for today
        show_start = datetime.combine(now.date(), start_time, timezone.utc)
        show_end = datetime.combine(now.date(), end_time, timezone.utc)
        
        # Handle overnight shows (end time next day)
        if show_end <= show_start:
            show_end += timedelta(days=1)
        
        # If start time has passed today, schedule for tomorrow
        if show_start <= now:
            show_start += timedelta(days=1)
            show_end += timedelta(days=1)
        
        # Align start time to 5-minute boundary
        aligned_minute = (show_start.minute // segment_minutes) * segment_minutes
        show_start = show_start.replace(minute=aligned_minute, second=0, microsecond=0)
        
        # Align end time to 5-minute boundary (round up)
        end_aligned_minute = ((show_end.minute + segment_minutes - 1) // segment_minutes) * segment_minutes
        if end_aligned_minute >= 60:
            show_end = show_end.replace(hour=show_end.hour + 1, minute=0, second=0, microsecond=0)
        else:
            show_end = show_end.replace(minute=end_aligned_minute, second=0, microsecond=0)
        
        duration_minutes = int((show_end - show_start).total_seconds() / 60)
        
        return show_start, show_end, duration_minutes
    
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
    
    def generate_continuous_filename(self, segment_start_time):
        """Generate filename for continuous recording segments"""
        date_str = segment_start_time.strftime("%Y%m%d")
        time_str = segment_start_time.strftime("%H%M")
        return f"{self.config['media_source_id']}_{date_str}_{time_str}.mp4"
    
    def generate_segment_list(self, show_start, show_end, segment_minutes):
        """Generate list of expected segments for scheduled recording"""
        segments = []
        current_time = show_start
        
        while current_time < show_end:
            date_str = current_time.strftime("%Y%m%d")
            time_str = current_time.strftime("%H%M")
            filename = f"{self.config['media_source_id']}_{date_str}_{time_str}.mp4"
            
            segments.append({
                'start_time': current_time,
                'filename': filename,
                'temp_filename': f"temp_segment_{len(segments)+1:03d}.mp4"
            })
            
            current_time += timedelta(minutes=segment_minutes)
        
        return segments
    
    def wait_for_time(self, target_time):
        """Wait until target time"""
        now = datetime.now(timezone.utc)
        if now < target_time:
            wait_seconds = (target_time - now).total_seconds()
            self.logger.info(f"Waiting {wait_seconds:.1f} seconds to start at {target_time.strftime('%H:%M:%S')} UTC")
            time.sleep(wait_seconds)
    
    def start_ffmpeg_continuous(self, segment_minutes):
        """Start FFmpeg for continuous recording (no end time)"""
        segment_seconds = segment_minutes * 60
        
        cmd = [
            'ffmpeg', '-y', '-re',
            '-i', self.config['srt_url'],
            '-c:v', self.config.get('video_codec', 'libx264'),
            '-preset', self.config.get('preset', 'medium'),
            '-b:v', self.config.get('video_bitrate', '2000k'),
            '-c:a', self.config.get('audio_codec', 'aac'),
            '-b:a', self.config.get('audio_bitrate', '128k'),
            '-f', 'segment',
            '-segment_time', str(segment_seconds),
            '-segment_format', 'mp4',
            '-segment_format_options', 'movflags=+faststart',
            '-reset_timestamps', '1',
            '-segment_wrap', '999999',  # Large number to avoid wrapping
            str(self.segments_dir / 'temp_segment_%06d.mp4')  # 6-digit counter for long recordings
        ]
        
        self.logger.info(f"Starting FFmpeg for CONTINUOUS recording with {segment_minutes} minute segments")
        self.logger.info(f"Command: {' '.join(cmd)}")
        
        self.ffmpeg_process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        return self.ffmpeg_process
    
    def start_ffmpeg_scheduled(self, duration_minutes, segment_minutes):
        """Start FFmpeg for scheduled recording (with end time)"""
        total_seconds = duration_minutes * 60
        segment_seconds = segment_minutes * 60
        
        cmd = [
            'ffmpeg', '-y', '-re',
            '-i', self.config['srt_url'],
            '-c:v', self.config.get('video_codec', 'libx264'),
            '-preset', self.config.get('preset', 'medium'),
            '-b:v', self.config.get('video_bitrate', '2000k'),
            '-c:a', self.config.get('audio_codec', 'aac'),
            '-b:a', self.config.get('audio_bitrate', '128k'),
            '-f', 'segment',
            '-segment_time', str(segment_seconds),
            '-segment_format', 'mp4',
            '-segment_format_options', 'movflags=+faststart',
            '-reset_timestamps', '1',
            '-t', str(total_seconds),
            str(self.segments_dir / 'temp_segment_%03d.mp4')
        ]
        
        self.logger.info(f"Starting FFmpeg for SCHEDULED recording: {duration_minutes} minutes with {segment_minutes} minute segments")
        self.logger.info(f"Command: {' '.join(cmd)}")
        
        self.ffmpeg_process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        return self.ffmpeg_process
    
    def monitor_continuous_segments(self, segment_minutes, recording_start_time):
        """Monitor segments for continuous recording"""
        self.logger.info("Starting continuous segment monitoring...")
        
        while self.running and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
            for temp_file in self.segments_dir.glob("temp_segment_*.mp4"):
                if temp_file.name not in self.processed_segments:
                    if self.is_file_complete(temp_file, segment_minutes):
                        # Calculate segment time based on sequence and start time
                        segment_time = self.calculate_segment_time(temp_file.name, recording_start_time, segment_minutes)
                        final_filename = self.generate_continuous_filename(segment_time)
                        final_path = self.segments_dir / final_filename
                        
                        temp_file.rename(final_path)
                        
                        self.logger.info(f"Renamed: {temp_file.name} → {final_filename}")
                        self.logger.info(f"Segment time: {segment_time.strftime('%H:%M:%S')} UTC")
                        
                        self.processed_segments.add(temp_file.name)
                        self.stats['segments_created'] += 1
                        
                        # Upload asynchronously
                        self.upload_executor.submit(self.upload_to_s3, final_path)
            
            time.sleep(2)
    
    def monitor_scheduled_segments(self, expected_segments):
        """Monitor segments for scheduled recording"""
        while self.running and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
            for temp_file in self.segments_dir.glob("temp_segment_*.mp4"):
                if temp_file.name not in self.processed_segments:
                    if self.is_file_complete(temp_file):
                        segment_info = self.match_temp_to_expected(temp_file.name, expected_segments)
                        
                        if segment_info:
                            final_path = self.segments_dir / segment_info['filename']
                            temp_file.rename(final_path)
                            
                            self.logger.info(f"Renamed: {temp_file.name} → {segment_info['filename']}")
                            self.logger.info(f"Segment time: {segment_info['start_time'].strftime('%H:%M:%S')} UTC")
                            
                            self.processed_segments.add(temp_file.name)
                            self.stats['segments_created'] += 1
                            
                            # Upload asynchronously
                            self.upload_executor.submit(self.upload_to_s3, final_path)
            
            time.sleep(2)
        
        # Process remaining segments
        time.sleep(5)
        self.process_remaining_scheduled_segments(expected_segments)
    
    def calculate_segment_time(self, temp_filename, recording_start_time, segment_minutes):
        """Calculate segment start time for continuous recording"""
        try:
            # Extract sequence number (temp_segment_000000.mp4 -> 0, temp_segment_000001.mp4 -> 1)
            seq_num = int(temp_filename.replace('temp_segment_', '').replace('.mp4', ''))
            segment_offset_minutes = seq_num * segment_minutes  # Fixed: seq_num starts at 0
            return recording_start_time + timedelta(minutes=segment_offset_minutes)
        except:
            # Fallback to current time aligned to boundary
            return self.get_next_aligned_time(segment_minutes)
    
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
    
    def is_file_complete(self, file_path, segment_minutes=5):
        """Check if file is complete and stable - flexible for different stream qualities"""
        try:
            # Check file exists
            if not file_path.exists():
                return False
                
            stat = file_path.stat()
            
            # Very small files are definitely incomplete (less than 50KB)
            if stat.st_size < 50000:
                return False
            
            # Check stability over time - file should stop growing
            size1 = stat.st_size
            time.sleep(3)  # Wait 3 seconds
            size2 = file_path.stat().st_size
            
            # If still growing significantly, not ready
            if size2 > size1 * 1.01:  # Allow for small increases (metadata writes)
                self.logger.debug(f"File {file_path.name} still growing: {size1:,} -> {size2:,} bytes")
                return False
            
            # Double-check stability with another wait
            time.sleep(2)
            size3 = file_path.stat().st_size
            
            if size3 != size2:
                self.logger.debug(f"File {file_path.name} still changing: {size2:,} -> {size3:,} bytes")
                return False
            
            # Calculate expected size range based on stream quality estimation
            # Very flexible ranges to accommodate different qualities:
            # - Low quality (480p@30fps): ~0.5-2MB per minute  
            # - Medium quality (720p@30fps): ~1-5MB per minute
            # - High quality (720p@60fps): ~2-10MB per minute
            # - Very high quality (1080p@60fps): ~5-25MB per minute
            
            min_size_per_minute = 200000   # 200KB per minute (very low quality streams)
            max_size_per_minute = 30000000 # 30MB per minute (very high quality streams)
            
            min_expected = segment_minutes * min_size_per_minute
            max_expected = segment_minutes * max_size_per_minute
            
            final_size = size3
            is_reasonable = min_expected <= final_size <= max_expected
            
            # Log detailed info for troubleshooting
            self.logger.info(f"File {file_path.name}: {final_size:,} bytes ({final_size/1024/1024:.2f}MB)")
            self.logger.info(f"Expected range for {segment_minutes}min: {min_expected/1024/1024:.1f}MB - {max_expected/1024/1024:.1f}MB")
            
            if is_reasonable:
                self.logger.info(f"✓ File {file_path.name} is complete and ready for upload")
                return True
            elif final_size < min_expected:
                self.logger.warning(f"⚠ File {file_path.name} seems too small ({final_size/1024/1024:.2f}MB) - might be incomplete")
                # For very small files, be more strict
                if final_size < 100000:  # Less than 100KB is definitely wrong
                    return False
                else:
                    # Allow small files in case it's a very low bitrate stream
                    self.logger.warning("Proceeding anyway (might be low-bitrate stream)")
                    return True
            else:
                self.logger.warning(f"⚠ File {file_path.name} is very large ({final_size/1024/1024:.2f}MB) - proceeding anyway")
                return True
                
        except Exception as e:
            self.logger.error(f"Error checking file completion for {file_path}: {e}")
            return False
    
    def upload_to_s3(self, file_path):
        """Upload file to S3 with retry logic"""
        for attempt in range(3):
            try:
                s3_key = file_path.name
                
                self.logger.info(f"Uploading {file_path.name} (attempt {attempt + 1})")
                
                self.s3.upload_file(
                    str(file_path),
                    self.config['s3_bucket'],
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'video/mp4',
                        'Metadata': {
                            'media_source_id': str(self.config['media_source_id']),
                            'upload_time': datetime.now(timezone.utc).isoformat(),
                            'recording_mode': self.recording_mode
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
    
    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        self.logger.info(f"Shutting down (signal {signum})...")
        self.running = False
        
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()
        
        self.upload_executor.shutdown(wait=True, timeout=30)
        
        # Final stats
        if self.stats['recording_start_time']:
            duration = datetime.now(timezone.utc) - self.stats['recording_start_time']
            self.logger.info(f"Recording duration: {duration}")
        
        self.logger.info("=== Final Statistics ===")
        self.logger.info(f"Segments created: {self.stats['segments_created']}")
        self.logger.info(f"Segments uploaded: {self.stats['segments_uploaded']}")
        self.logger.info(f"Upload failures: {self.stats['upload_failures']}")
        self.logger.info("Shutdown complete")
        sys.exit(0)
    
    def record_continuous(self, segment_minutes=5):
        """Record continuously until stopped"""
        self.recording_mode = "continuous"
        self.logger.info(f"Starting CONTINUOUS recording with {segment_minutes}-minute segments")
        
        # Wait for next 5-minute boundary
        recording_start_time = self.get_next_aligned_time(segment_minutes)
        self.wait_for_time(recording_start_time)
        
        self.stats['recording_start_time'] = recording_start_time
        
        self.logger.info(f"Continuous recording started at: {recording_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        self.logger.info("Recording will continue until stopped (Ctrl+C)")
        
        # Start FFmpeg
        self.start_ffmpeg_continuous(segment_minutes)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self.monitor_continuous_segments, 
            args=(segment_minutes, recording_start_time), 
            daemon=True
        )
        monitor_thread.start()
        
        # Wait indefinitely (until Ctrl+C or signal)
        try:
            while self.running and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                time.sleep(60)  # Check every minute
                
                # Log stats every hour
                if datetime.now(timezone.utc).minute == 0:
                    self.logger.info(f"Continuous recording stats: {self.stats['segments_created']} segments created, "
                                   f"{self.stats['segments_uploaded']} uploaded")
            
            if self.ffmpeg_process and self.ffmpeg_process.poll() is not None:
                self.logger.error("FFmpeg process ended unexpectedly")
                
        except KeyboardInterrupt:
            self.logger.info("Continuous recording stopped by user")
        
        self.running = False
        monitor_thread.join(timeout=30)
    
    def record_scheduled(self, start_time_str, end_time_str, segment_minutes=5):
        """Record a scheduled show using start/end times"""
        self.recording_mode = "scheduled"
        self.logger.info(f"Starting SCHEDULED recording: {start_time_str} to {end_time_str}")
        
        # Calculate aligned times
        show_start, show_end, duration = self.calculate_aligned_times(
            start_time_str, end_time_str, segment_minutes
        )
        
        self.logger.info(f"Aligned times:")
        self.logger.info(f"  Start: {show_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        self.logger.info(f"  End: {show_end.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        self.logger.info(f"  Duration: {duration} minutes")
        
        # Generate expected segments
        expected_segments = self.generate_segment_list(show_start, show_end, segment_minutes)
        
        self.logger.info("Expected segments:")
        for i, segment in enumerate(expected_segments, 1):
            self.logger.info(f"  {i}: {segment['filename']} ({segment['start_time'].strftime('%H:%M')})")
        
        # Wait for start time
        self.wait_for_time(show_start)
        
        self.stats['recording_start_time'] = show_start
        
        # Start recording
        self.start_ffmpeg_scheduled(duration, segment_minutes)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self.monitor_scheduled_segments, 
            args=(expected_segments,), 
            daemon=True
        )
        monitor_thread.start()
        
        # Wait for completion
        try:
            self.ffmpeg_process.wait()
            self.logger.info("Scheduled recording completed successfully")
        except KeyboardInterrupt:
            self.logger.info("Scheduled recording interrupted")
        
        self.running = False
        monitor_thread.join(timeout=30)
        
        # Final stats
        self.logger.info("=== Scheduled Recording Complete ===")
        self.logger.info(f"Segments created: {self.stats['segments_created']}")
        self.logger.info(f"Segments uploaded: {self.stats['segments_uploaded']}")
        self.logger.info(f"Upload failures: {self.stats['upload_failures']}")

def main():
    parser = argparse.ArgumentParser(description='Unified SRT Recorder - Continuous & Scheduled Modes')
    
    subparsers = parser.add_subparsers(dest='mode', help='Recording mode')
    
    # Continuous mode
    continuous_parser = subparsers.add_parser('continuous', help='Record continuously until stopped')
    continuous_parser.add_argument('--segment-duration', type=int, default=5,
                                 help='Segment duration in minutes (default: 5)')
    continuous_parser.add_argument('--config', default='config.json', help='Config file path')
    
    # Scheduled mode
    scheduled_parser = subparsers.add_parser('scheduled', help='Record with start/end times')
    scheduled_parser.add_argument('start_time', help='Start time (HH:MM format)')
    scheduled_parser.add_argument('end_time', help='End time (HH:MM format)')
    scheduled_parser.add_argument('--segment-duration', type=int, default=5,
                                help='Segment duration in minutes (default: 5)')
    scheduled_parser.add_argument('--config', default='config.json', help='Config file path')
    
    args = parser.parse_args()
    
    if not args.mode:
        parser.print_help()
        print("\nExamples:")
        print("  Continuous:  python3 unified_recorder.py continuous --segment-duration 5")
        print("  Scheduled:   python3 unified_recorder.py scheduled 18:00 18:30 --segment-duration 5")
        sys.exit(1)
    
    try:
        recorder = UnifiedSRTRecorder(args.config)
        
        if args.mode == 'continuous':
            recorder.record_continuous(args.segment_duration)
        elif args.mode == 'scheduled':
            recorder.record_scheduled(args.start_time, args.end_time, args.segment_duration)
            
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
