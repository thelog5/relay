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

output "load_test" {
  value = "loadtest/run.sh http://${aws_eip.relay.public_ip}:8080 ${var.instance_type}"
}
