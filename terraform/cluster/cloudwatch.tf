# Container logs ship here via the Docker awslogs driver (set in cache user-data). One log group;
# each node streams under its instance id. Alarms live in observability.tf (Stage 6).
resource "aws_cloudwatch_log_group" "cache" {
  name              = "/kvcache/cache"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "etcd" {
  name              = "/kvcache/etcd"
  retention_in_days = 7
}
