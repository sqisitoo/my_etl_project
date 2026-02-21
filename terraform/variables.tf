variable "aws_region" {
  description = "AWS Region to deploy on"
  default     = "eu-north-1"
}

variable "aws_profile" {
  description = "AWS CLI Profile"
  default     = "my-pet-project"
}

variable "enable_nat_gateway" {
  description = "Enable NAT Gateway?"
  type        = bool
  default     = false
}

variable "create_bastion" {
  description = "Create Bastion Host for DB access?"
  type        = bool
  default     = false
}

variable "allowed_ssh_cidrs" {
  description = "List of IPs to access the bastion"
  type        = list(string)
  default     = []
}

variable "api_base_url" {
  description = "OpenWeather API endpoint"
  type        = string
}

variable "openweather_api_key" {
  description = "OpenWeather API Key"
  type        = string
  sensitive   = true
}