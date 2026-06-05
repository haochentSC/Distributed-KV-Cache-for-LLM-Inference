# ECR repository for the cache-server image. Push to it with scripts/push-image.sh before apply.
resource "aws_ecr_repository" "cache" {
  name                 = "kvcache/cache-server"
  image_tag_mutability = "MUTABLE" # we re-push :latest during iteration
  force_delete         = true      # let `terraform destroy` clean it even with images present

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keep the repo from accumulating untagged layers across re-pushes.
resource "aws_ecr_lifecycle_policy" "cache" {
  repository = aws_ecr_repository.cache.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "expire untagged images after 3 days"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}
