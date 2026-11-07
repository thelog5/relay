variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type. t3.small (2 vCPU, 2 GiB) is about $15/month on demand. Record this next to the load test numbers."
  type        = string
  default     = "t3.small"
}

variable "repo_url" {
  description = "Git URL the instance clones and runs with docker compose."
  type        = string
  default     = "https://github.com/thelog5/relay.git"
}

variable "repo_ref" {
  description = "Branch, tag, or commit to deploy."
  type        = string
  default     = "main"
}

variable "kill_token" {
  description = "Shared secret required by POST /workers/:id/kill on the public demo. Empty disables the check."
  type        = string
  default     = ""
  sensitive   = true
}

variable "admin_cidr" {
  description = "CIDR allowed to reach SSH, Prometheus (9090) and Grafana (3000). Set to your IP, e.g. 203.0.113.7/32."
  type        = string
}

variable "public_dashboard" {
  description = "Open the dashboard port (8080) to the whole internet. If false, only admin_cidr can reach it."
  type        = bool
  default     = true
}

variable "ssh_public_key" {
  description = "Optional SSH public key for the ubuntu user. Leave empty to use SSM Session Manager only."
  type        = string
  default     = ""
}

variable "root_volume_gb" {
  description = "Root EBS volume size."
  type        = number
  default     = 20
}
