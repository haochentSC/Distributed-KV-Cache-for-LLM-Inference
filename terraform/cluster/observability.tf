# CloudWatch alarms (the cloud-native half of the observability story; Prometheus/Grafana cover the
# app-level metrics — hit rate, evictions, replication lag). CloudWatch here watches INFRA LIVENESS:
# an EC2 StatusCheckFailed means the instance/host is unhealthy. etcd liveness matters most (quorum),
# so it gets its own alarms; the cache fleet gets them too.
#
# Alarms notify an SNS topic; set -var alarm_email to get an email subscription (otherwise the alarm
# still fires and is visible in the console, just without a notification).

variable "alarm_email" {
  description = "Email to subscribe to CloudWatch alarms. Empty = no subscription (alarms still fire in-console)."
  type        = string
  default     = ""
}

resource "aws_sns_topic" "alarms" {
  name = "kvcache-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# etcd node health — losing one is a quorum risk (ADR 0009).
resource "aws_cloudwatch_metric_alarm" "etcd_health" {
  count               = var.etcd_count
  alarm_name          = "kvcache-etcd-${count.index}-unhealthy"
  alarm_description   = "etcd-${count.index} failed an EC2 status check (quorum risk)."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 60
  evaluation_periods  = 2
  dimensions          = { InstanceId = aws_instance.etcd[count.index].id }
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

# Cache node health — a Spot reclaim/crash shows here; failover keeps correctness, but a fleet-wide
# pattern is worth a page.
resource "aws_cloudwatch_metric_alarm" "cache_health" {
  count               = var.cache_count
  alarm_name          = "kvcache-cache-${count.index}-unhealthy"
  alarm_description   = "cache-${count.index} failed an EC2 status check (Spot reclaim or crash)."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 60
  evaluation_periods  = 2
  dimensions          = { InstanceId = aws_instance.cache[count.index].id }
  alarm_actions       = [aws_sns_topic.alarms.arn]
}
