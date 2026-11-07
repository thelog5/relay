# One EC2 instance running the whole docker compose stack (R11).

data "aws_vpc" "default" {
  default = true
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "relay" {
  name_prefix = "relay-"
  description = "Relay demo: dashboard/API public, admin ports restricted"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Relay API and dashboard"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.public_dashboard ? "0.0.0.0/0" : var.admin_cidr]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Grafana"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Prometheus"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_key_pair" "relay" {
  count      = var.ssh_public_key == "" ? 0 : 1
  key_name   = "relay-demo"
  public_key = var.ssh_public_key
}

# Lets you open a shell with SSM Session Manager instead of SSH.
resource "aws_iam_role" "relay" {
  name_prefix = "relay-ec2-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.relay.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "relay" {
  name_prefix = "relay-ec2-"
  role        = aws_iam_role.relay.name
}

resource "aws_instance" "relay" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.relay.id]
  iam_instance_profile   = aws_iam_instance_profile.relay.name
  key_name               = var.ssh_public_key == "" ? null : aws_key_pair.relay[0].key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url   = var.repo_url
    repo_ref   = var.repo_ref
    kill_token = var.kill_token
  })
  user_data_replace_on_change = true

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "relay-demo"
  }
}

resource "aws_eip" "relay" {
  instance = aws_instance.relay.id
  domain   = "vpc"
  tags = {
    Name = "relay-demo"
  }
}
