output "public_ip" {
  value = aws_eip.relay.public_ip
}

output "dashboard_url" {
  value = "http://${aws_eip.relay.public_ip}:8080"
}

output "grafana_url" {
  value = "http://${aws_eip.relay.public_ip}:3000"
}

output "instance_type" {
  description = "Write this next to the load test numbers in the README."
  value       = var.instance_type
}

output "ssm_session" {
  value = "aws ssm start-session --target ${aws_instance.relay.id} --region ${var.region}"
}

output "instance_id" {
  value = aws_instance.relay.id
}

output "load_test" {
  description = "Run from the repo root on your laptop. Add KILL_TOKEN=<your token> if you set one."
  value       = "INSTANCE_ID=${aws_instance.relay.id} AWS_REGION=${var.region} loadtest/run.sh http://${aws_eip.relay.public_ip}:8080 ${var.instance_type}"
}
