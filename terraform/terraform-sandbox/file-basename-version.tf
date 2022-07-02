output "path_module" {
  value = path.module
}

output "fileset" {
  value = fileset(path.module, "my-*.jar")
}

output "first" {
  value = tolist(fileset(path.module, "my-*.jar"))[0]
}

output "basename" {
  value = basename(tolist(fileset(path.module, "my-*.jar"))[0])
}

output "version" {
  value = element(regex("(\\d+(\\.\\d+){0,2}(-SNAPSHOT)?)", basename(tolist(fileset(path.module, "my-*.jar"))[0])),0)
}
