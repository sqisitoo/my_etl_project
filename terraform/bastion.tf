resource "tls_private_key" "bastion" {
  count     = var.create_bastion ? 1 : 0
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "bastion_key" {
  count      = var.create_bastion ? 1 : 0
  key_name   = "bastion_key"
  public_key = tls_private_key.bastion[0].public_key_openssh
}

resource "aws_secretsmanager_secret" "bastion_private_key" {
  count                   = var.create_bastion ? 1 : 0
  name                    = "airflow-bastion"
  description             = "SSH private key for bastion host"
  recovery_window_in_days = 0

  tags = {
    Name = "Bastion Private Key"
  }
}

resource "aws_secretsmanager_secret_version" "bastion_private_key" {
  count = var.create_bastion ? 1 : 0

  secret_id     = aws_secretsmanager_secret.bastion_private_key[0].id
  secret_string = tls_private_key.bastion[0].private_key_pem
}


data "aws_ami" "amazon_linux" {
  count       = var.create_bastion ? 1 : 0
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "bastion_sg" {
  count  = var.create_bastion ? 1 : 0
  vpc_id = aws_vpc.airflow.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bastion" {
  count = var.create_bastion ? 1 : 0

  ami           = data.aws_ami.amazon_linux[0].id
  instance_type = "t3.micro"
  subnet_id     = aws_subnet.public_1.id

  key_name                    = aws_key_pair.bastion_key[0].key_name
  vpc_security_group_ids      = [aws_security_group.bastion_sg[0].id]
  associate_public_ip_address = true

  tags = {
    Name = "airflow-bastion"
  }
}

output "bastion_public_ip" {
  description = "Public IP of the bastion host"
  value       = var.create_bastion ? aws_instance.bastion[0].public_ip : null
}

output "bastion_instance_id" {
  description = "Bastion instance ID"
  value       = var.create_bastion ? aws_instance.bastion[0].id : null
}

output "bastion_ssh_key_secret_name" {
  description = "Name of Secrets Manager secret containing bastion private key"
  value       = var.create_bastion ? aws_secretsmanager_secret.bastion_private_key[0].name : null
}

output "bastion_connection_instructions" {
  description = "Instructions to connect to bastion"
  value       = <<EOT
%{if var.create_bastion}
1. Get the private key:
   aws secretsmanager get-secret-value \
      --secret-id ${aws_secretsmanager_secret.bastion_private_key[0].name} \
      --query SecretString \
      --output text > bastion_key.pem

2. Set correct permissions:
    chmod 600 bastion_key.pem

3. Connect to bastion:
    ssh -i bastion_key.pem ec2-user@${aws_instance.bastion[0].public_ip}

4. If you want to connect to rds throw bastion:
    ssh -i bastion_key.pem -L 5432:${aws_db_instance.default.endpoint} \ 
      ec2-user@${aws_instance.bastion[0].public_ip}

4. Clean up:
   rm bastion_key.pem
%{else}
No bastion created.
%{endif}
EOT
}