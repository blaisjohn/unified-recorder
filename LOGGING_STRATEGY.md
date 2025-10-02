# Logging Strategy for 24/7 Production Operation

## Overview
The recorder has two logging systems optimized for continuous 24/7 operation with automatic cleanup.

---

## 1. Application Logs (Main Recorder)

### Location:
- **Kubernetes/Docker**: `stdout` only (captured by Kubernetes logging)
- **Local**: `stream_ingestion_engine.log` + `stdout`

### Rotation:
- **Max file size**: 10MB per file
- **Backup count**: 5 files
- **Total max size**: 60MB (10MB current + 50MB backups)
- **Auto-cleanup**: Oldest files deleted automatically

### What's Logged:
- ✅ Startup configuration
- ✅ Stream detection results
- ✅ Segment completion (every 5 minutes)
- ✅ Upload successes/failures
- ✅ Connection retries and reconnections
- ✅ Critical errors
- ✅ Stats every 5 minutes
- ✅ Disk space warnings

### What's NOT Logged (to reduce noise):
- ❌ Debug-level details (unless level changed)
- ❌ Every FFmpeg output line
- ❌ Repeated errors (throttled)

---

## 2. FFmpeg Debug Logs

### Production Mode (Default):
- **File logging**: DISABLED
- **To stdout**: Only critical events
- **Throttling**: Timestamp errors logged max once per 5 minutes
- **Disk usage**: ~0 MB

### Debug Mode (opt-in):
Set environment variable: `FFMPEG_DEBUG_MODE=true`

- **File location**: `segments/ffmpeg_debug.log`
- **Max file size**: 10MB per file
- **Backup count**: 2 files (.log, .log.1, .log.2)
- **Total max size**: 30MB
- **Auto-cleanup**: Yes
- **Contents**: Every FFmpeg stderr line with timestamps

---

## 3. Log Noise Reduction

### Filtered Out (in production mode):
These errors are suppressed as they're expected with problematic SRT sources:
- `decode_slice_header` errors
- `timestamp discontinuity` (very common, logged once per 5 min)
- `non-monotonic dts` (handled automatically)
- `packet corrupt` (expected with bad timestamps)
- `pes packet size mismatch`

### Still Logged:
- Actual connection failures
- Segment creation failures
- Upload failures
- Performance issues (speed < 0.95x)
- Real FFmpeg errors

---

## 4. Kubernetes Recommendations

### For Production Deployment:

```yaml
env:
  - name: FFMPEG_DEBUG_MODE
    value: "false"  # Keep disabled unless debugging

# Kubernetes will capture stdout
# Use kubectl logs or logging aggregator (ELK, Splunk, CloudWatch)
```

### For Debugging Issues:

```yaml
env:
  - name: FFMPEG_DEBUG_MODE
    value: "true"  # Enable temporary for troubleshooting

# Check logs with:
# kubectl exec -it <pod> -- ls -lh /app/segments/ffmpeg_debug.log*
```

---

## 5. Monitoring Key Metrics

### Via Application Logs (every 5 minutes):
```
Stats [24/7 Operation]: Created: 188, Downmixed: 188, Uploaded: 188, Failed: 0, Reconnections: 0
```

### What to Alert On:
- `Failed: > 0` - Upload failures
- `Reconnections: > 5` - Unstable source
- `"CRITICAL: Only XGB disk space remaining"`
- `"FFmpeg slow: speed=0.XX"` - Performance issues
- Long gaps between "Segment completed" logs (>6 minutes)

---

## 6. Disk Space Management

### Automatic Cleanup:
1. **Segments**: Deleted immediately after successful S3 upload
2. **Failed segments**: Retried on next startup, then uploaded and deleted
3. **Application logs**: Auto-rotated at 10MB (max 60MB total)
4. **FFmpeg debug logs**: Auto-rotated at 10MB (max 30MB total in debug mode)

### Emergency Cleanup:
If disk space <1GB:
- Keeps only 10 most recent segment files
- Logs critical warning
- Automatic cleanup runs

### Total Disk Usage (steady state):
- **Segments**: ~100-200MB (1-2 segments waiting for upload)
- **App logs**: ~60MB max
- **FFmpeg logs**: 0MB (production) or 30MB (debug mode)
- **Total**: ~150-300MB typical

---

## 7. Long-term Operation (Years)

### Memory Leaks Prevention:
- ✅ Segment tracking limited to 100 entries (prevents unbounded growth)
- ✅ Error throttle dict doesn't grow (fixed keys)
- ✅ Thread pools have fixed size
- ✅ Files deleted after upload (no accumulation)

### Log Rotation:
- ✅ Automatic (no manual intervention needed)
- ✅ Size-based (not time-based) - works for any recording duration
- ✅ Old logs automatically deleted

### Graceful Degradation:
- If S3 upload fails → segments remain on disk for retry
- If disk fills → emergency cleanup triggers
- If FFmpeg crashes → automatic reconnection
- If source has bad timestamps → recorder adapts and continues

---

## 8. Troubleshooting Commands

### Check current log sizes:
```bash
# In container
ls -lh /app/segments/*.log*
du -sh /app/segments/

# Local
ls -lh stream_ingestion_engine.log*
ls -lh segments/ffmpeg_debug.log*
```

### Enable debug mode temporarily:
```bash
export FFMPEG_DEBUG_MODE=true
python3 unified_recorder.py --config config.json
```

### View recent errors only:
```bash
kubectl logs <pod> | grep -i error | tail -20
```

### Monitor in real-time:
```bash
kubectl logs -f <pod> | grep -E "Segment|Upload|Error|Stats"
```

---

## Summary

**Production mode** (default):
- Minimal disk usage (~150-300MB)
- Clean, actionable logs
- Automatic rotation and cleanup
- Designed for years of continuous operation
- No manual maintenance required

**Debug mode** (when needed):
- Full FFmpeg output captured
- Max 30MB additional disk usage
- Useful for diagnosing source stream issues
- Re-disable after troubleshooting
