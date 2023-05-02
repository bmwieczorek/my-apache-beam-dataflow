resource "null_resource" "my_command" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "echo 'hello ${var.my_required_variable} and ${var.my_optional_variable}'"
#    command = "echo 'hello ${var.my_required_variable}'"
  }
}
