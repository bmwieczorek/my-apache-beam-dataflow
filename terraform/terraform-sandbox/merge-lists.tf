locals {
  my_default_list = ["a", "b"]
  my_additional_empty_list = []
  my_additional_non_empty_list = [ "c", "d" ]
}

output "merge_with_empty_list" {
  value = concat(local.my_default_list, local.my_additional_empty_list)
}

output "merge_with_non_empty_list" {
  value = concat(local.my_default_list, local.my_additional_non_empty_list)
}