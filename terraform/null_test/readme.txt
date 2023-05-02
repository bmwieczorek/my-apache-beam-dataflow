# my_optional_variable is declares with default value null in variables.tf

# Error: Invalid template interpolation value: The expression result is null. Cannot include a null value in a string template.
terraform apply -auto-approve -var-file=env/dev__only_req.tfvars

# No error
# null_resource.my_command (local-exec): hello required and optional
terraform apply -auto-approve -var-file=env/dev__req_and_opt.tfvars
