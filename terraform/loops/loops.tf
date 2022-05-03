locals {
  my_string_list = [ "a", "b" ]
  my_int_list = [ 1, 2 ]
  my_map = { "a" = 1, "b" = 2 }
  my_nested_list = [ ["a","aa"],["b","bb"],["c","cc"] ]
  my_person_list = [ {name="Foo",age=10},{name="Bar",age=20} ]
  my_nested_map = {
    key-a = {
      my_inner_value = "a"
      my_inner_list = [ "A", "AA", "AAA" ]
    },
    key-b = {
      my_inner_value = "b"
      my_inner_list = [ "B", "BB", "BBB" ]
    },
    key-c = {
      my_inner_value = "c"
      my_inner_list = [ "C", "CC", "CCC" ]
    }
  }

  inner_list = [
    for k,v in var.my_nested_map: [
//      for e in lookup(v, "my_inner_list", {}): { name = e, value = v.my_inner_value }
      for e in v.my_inner_list: { name = e, value = v.my_inner_value }
    ]
  ]
}

output "lookup" {
  value = lookup(local.my_map, "a", -1)
}

output "lookup-default" {
  value = lookup(local.my_map, "z", -1)
}

output "inner_list" {
  value = local.inner_list
}

output "inner_list2" {
  value = [
//    for k,v in var.my_nested_map: [
    for k,v in local.my_nested_map: [
      for e in lookup(v, "my_inner_list", {}): { name = e, value = v.my_inner_value }
    ]
  ]
}

output "list_element_by_index" {
  value = local.my_string_list[0]
}

output "list_length" {
  value = length(local.my_string_list)
}

output "new_my_list" {
  // code after : represents how to transform existing list element,
  // output value is new list as surrounded by []
  value = [ for e in local.my_string_list : "Hello ${upper(e)}" ]
}

output "flatten_map" {
  value = flatten(local.my_nested_list)
}

output "map_value_by_key" {
  value = local.my_map["a"]
}


output "new_my_map" {
  // code after : represents how to create an new new_key => new_value entry based on existing k,v from my_map,
  // output value is map as surrounded by {}
  value = { for k, v in local.my_map : upper(k) => (v + v) }
}

output "list_to_map" {
  value = { for e in local.my_string_list: e => upper(e) }
}

output "list_to_map_with_index" {
  value = { for index, e in local.my_string_list: e => index }
}

output "map_to_list" {
  value = [ for k,v in local.my_map: "${k}${v}"]
}

output "simple_type_list_to_complex_type_list" {
  value = [ for e in local.my_string_list: { lower = e, upper = upper(e) } ] // create nested record with lower and upper properties based list element
}

output "flatten_list_of_list_to_list" {
  value = flatten([ for s in local.my_string_list: [ for n in local.my_int_list: { name = "${s}_${n}"} ]])
}

output "flatten_list_of_list_to_map" {
  value = { for v in flatten([ for s in local.my_string_list: [ for n in local.my_int_list: { id = "${s}${n}", description = "Hello ${s} ${n}"} ]]): v["id"] => v }
}

output "keyB" {
  value = local.my_nested_map["key-c"] // access my key name
}

resource "null_resource" "null" {
  for_each = local.my_map
}

resource "google_storage_bucket" "list-bucket" {
  for_each = toset(local.my_string_list)
  name = "bartek-list-${each.key}-bucket"
  project = var.project
  location = "us"
}

resource "google_storage_bucket" "map-bucket" {
  for_each = local.my_map
  name = "bartek-map-${each.value}-bucket"
//  name = "bartek-map-${each.key}-bucket"
  project = var.project
  location = "us"
}

resource "google_storage_bucket" "nested-map-bucket" {
  for_each = local.my_nested_map
  name = "bartek-nested-map-${each.value.my_inner_value}-bucket"
  project = var.project
  location = "us"
}


variable "my_nested_map" {
  type = map(object({ my_inner_value = string,my_inner_list=list(string)}))
  default = {
    key-z = {
      my_inner_value = "z"
      my_inner_list = [ "Z", "ZZ", "ZZZ" ]
    }
  }
}
resource "google_storage_bucket" "variable-nested-map-bucket" {
  for_each = var.my_nested_map
  name = "bartek-variable-nested-map-${each.value.my_inner_value}-bucket"
  project = var.project
  location = "us"
}
