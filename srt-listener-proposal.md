# SRT Listener Implementation Proposal
## External Broadcaster Connectivity for Unified Recorder

---

## Executive Summary

We need to enable external broadcasters to send SRT streams to our unified recorder pods running in Kubernetes. This document outlines implementation approaches, with a focus on leveraging existing infrastructure and minimizing changes.

**Key Requirements:**
- Accept incoming SRT streams from external broadcasters
- Handle 50-100 Mbps video feeds
- Provide stable URLs/IPs to broadcasters
- Support SRT encryption (passphrase authentication)
- Align recordings to 5-minute clock boundaries

---

## Current Architecture

- **Platform:** Kubernetes on AWS and Azure (aiware)
- **Existing Infrastructure:** LoadBalancer capabilities in both clouds
- **Recording System:** Unified SRT Recorder (Python/FFmpeg)
- **Storage:** S3 for segment storage
- **Current Mode:** SRT caller (outbound connections only)

---

## Proposed Solution: SRT Listener Mode

### How It Works

1. **Recorder pod starts in listener mode** - waits for incoming connections
2. **Broadcaster connects** using provided URL/IP
3. **Connection established** - recorder buffers stream
4. **Recording starts** at next 5-minute boundary (e.g., :00, :05, :10)
5. **Segments created** every 5 minutes, uploaded to S3

### Configuration Changes

```json
{
  "connection_mode": "listener",
  "listener_port": 9999,
  "listener_passphrase": "secure-password-here",
  "listener_external_url": "srt://ingest.company.com:9999",
  "segment_length_in_seconds": 300
}
```

---

## Implementation Options

### Option 1: Native Kubernetes LoadBalancer (Recommended)

**How it works:**
- Deploy K8s Service with `type: LoadBalancer`
- K8s provisions external IP via AWS/Azure
- Direct path: Broadcaster → LoadBalancer → Pod

**Deployment:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: srt-listener
spec:
  type: LoadBalancer
  ports:
  - port: 9999
    protocol: UDP
  selector:
    app: srt-recorder
```

**Pros:**
- Uses existing LoadBalancer infrastructure
- Native K8s integration
- No additional dependencies
- Full bandwidth support
- Integrated monitoring/logging

**Cons:**
- May need firewall rule for UDP port 9999
- ~$20-30/month per LoadBalancer (already budgeted)

**Timeline:** 1-2 days to implement and test

---

### Option 2: Cloudflare Tunnel

**How it works:**
- Add Cloudflare sidecar container to pod
- Creates outbound tunnel (no inbound firewall changes)
- Broadcaster connects via Cloudflare network

**Deployment:**
```yaml
containers:
- name: recorder
  image: srt-recorder:latest
- name: cloudflared
  image: cloudflare/cloudflared:latest
  env:
  - name: TUNNEL_TOKEN
    value: "token-here"
```

**Pros:**
- Zero infrastructure changes
- Free for our bandwidth needs
- DDoS protection included
- Professional URLs (ingest.company.com)

**Cons:**
- External dependency
- Traffic routes through Cloudflare
- Additional container complexity

**Timeline:** 1 day to implement and test

---

### Option 3: VPS Relay

**How it works:**
- Deploy small VPS as UDP relay
- Forward traffic to K8s NodePort
- Full control over routing

**Pros:**
- Complete control
- No K8s infrastructure changes
- Can handle multiple streams

**Cons:**
- Additional infrastructure to manage
- $20-50/month for VPS
- Another point of failure

---

## Comparison Matrix

| Criteria | K8s LoadBalancer | Cloudflare Tunnel | VPS Relay |
|----------|------------------|-------------------|-----------|
| **Infrastructure Changes** | Firewall rule | None | None |
| **Cost** | $20-30/mo (existing) | Free | $20-50/mo |
| **Bandwidth** | Unlimited | Unlimited | ~1-5TB/mo |
| **Setup Time** | 1-2 days | 1 day | 2-3 days |
| **Maintenance** | None (managed) | Minimal | Manual |
| **Security** | Native K8s | Cloudflare managed | Self-managed |
| **Monitoring** | Existing tools | Basic | Custom |
| **URL Format** | IP or DNS | Custom domain | IP or DNS |

---

## Bandwidth Considerations

**100 Mbps SRT Stream:**
- 12.5 MB/second
- 750 MB/minute
- 45 GB/hour
- 1.08 TB/day

All proposed solutions handle this bandwidth:
- ✅ LoadBalancer: No limits
- ✅ Cloudflare: No limits (CDN infrastructure)
- ✅ VPS: Depends on provider (typically 1-5TB/month)

---

## Security Considerations

1. **SRT Encryption**
   - Passphrase-based authentication
   - AES encryption for stream content
   - Configurable key length (16/24/32 bytes)

2. **Network Security**
   - LoadBalancer: Security groups/NSGs
   - Cloudflare: Built-in DDoS protection
   - IP allowlisting available

3. **Access Control**
   - Unique passphrases per broadcaster
   - Connection logging and monitoring
   - Automatic disconnection on invalid auth

---

## Recording Architecture

### Connection Flow
```
Broadcaster connects → Listener accepts → Buffer stream → 
Wait for boundary → Start recording → Create 5-min segments → Upload to S3
```

### Segment Alignment
- Always align to 5-minute clock boundaries
- Example: Connect at 14:32:17 → Start recording at 14:35:00
- Ensures predictable filenames and consistent processing

### Resilience
- Automatic reconnection handling
- Segment recovery on connection loss
- Continued recording during brief disconnects

---

## Recommendation

**Primary: Native K8s LoadBalancer**
- Leverages existing investment in aiware infrastructure
- No new dependencies or services
- Proven, scalable solution
- Minimal operational overhead

**Backup: Cloudflare Tunnel**
- If firewall changes are problematic
- Provides immediate solution with zero infrastructure impact
- Can be implemented quickly as POC

---

## Implementation Plan

### Phase 1: Proof of Concept (Week 1)
- [ ] Update recorder code for listener mode
- [ ] Deploy test pod with LoadBalancer
- [ ] Configure security groups for UDP 9999
- [ ] Test with sample SRT stream

### Phase 2: Production Readiness (Week 2)
- [ ] Add connection monitoring and alerting
- [ ] Implement segment alignment logic
- [ ] Set up DNS records for broadcaster URLs
- [ ] Create broadcaster documentation

### Phase 3: Rollout (Week 3)
- [ ] Deploy to staging environment
- [ ] Test with actual broadcaster
- [ ] Monitor performance and stability
- [ ] Deploy to production

---

## Cost Analysis

### Using Existing LoadBalancer
- **Additional Cost:** $0 (using existing infrastructure)
- **Bandwidth:** Included in current AWS/Azure agreements
- **Storage:** S3 costs unchanged (same segments)

### Alternative Options
- **Cloudflare Tunnel:** Free (legitimate use)
- **VPS Relay:** $20-50/month + bandwidth
- **Multiple Regions:** Multiply costs by region count

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| LoadBalancer provisioning fails | Low | Medium | Use Cloudflare as backup |
| Bandwidth overwhelming | Low | High | Rate limiting, monitoring |
| Security breach | Low | High | Strong passphrases, IP filtering |
| Connection stability | Medium | Medium | Reconnection logic, buffering |

---

## Questions for Discussion

1. **Infrastructure Approval**
   - Can we add UDP port 9999 to security groups?
   - Any concerns with LoadBalancer approach?

2. **Scale Considerations**
   - How many concurrent broadcasters expected?
   - Multi-region active/active or active/passive?

3. **Operational Requirements**
   - SLA expectations for broadcaster connectivity?
   - Monitoring/alerting integration preferences?

4. **Security Requirements**
   - Additional authentication beyond SRT passphrase?
   - Specific compliance requirements?

---

## Next Steps

1. **Confirm approach** (LoadBalancer vs Cloudflare)
2. **Security review** of chosen solution
3. **Begin Phase 1** implementation
4. **Schedule broadcaster testing**

---

## Appendix A: Technical Details

### AWS LoadBalancer Specifics
- Type: Network Load Balancer (NLB) for UDP support
- Target Group: Instance-based
- Health Checks: UDP or TCP
- Cross-zone: Enabled for HA

### Azure LoadBalancer Specifics
- SKU: Standard (not Basic)
- Backend Pool: Node-based
- Health Probes: UDP support
- Availability Zones: Supported

### SRT Protocol Details
- Default port: 9999 (configurable)
- Transport: UDP
- Latency: 120ms default buffer
- Encryption: AES-128/192/256
- Passphrase: Up to 79 characters

---

## Appendix B: Broadcaster Instructions

### Example Configuration

**For OBS Studio:**
- Server: `srt://ingest.company.com:9999`
- Stream Key: `passphrase=your-secure-password`

**For vmix:**
- URL: `srt://ingest.company.com:9999?mode=caller&passphrase=your-secure-password`

**For FFmpeg:**
```bash
ffmpeg -i input.mp4 -c copy -f mpegts \
  "srt://ingest.company.com:9999?mode=caller&passphrase=your-secure-password"
```

---

## Contact

- **Technical Lead:** [Your Name]
- **Project:** Unified SRT Recorder - Listener Mode
- **Timeline:** 2-3 weeks for full implementation