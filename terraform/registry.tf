resource "aws_ecr_repository" "airflow" {
  name                 = "airflow-repo"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "airflow-repo"
  }
}

resource "aws_ecr_lifecycle_policy" "airflow_policy" {
  repository = aws_ecr_repository.airflow.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = {
        type = "expire"
      }

    }]


  })
}