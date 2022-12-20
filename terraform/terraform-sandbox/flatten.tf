locals {
  // x, a and b are maps / objects
  x = {
    a = {
      A=[1]
      AA=[11,12]
    },
    b = {
      B=[2]
      BB=[21,22]
    }
  }
}

output "flattenSimpleArray" {
  value = flatten(["a", "b"])
}

output "flattenArrayOfArrays" {
  value = flatten([["a", "b"], [], ["c"]])
}

output "nestedWithoutFlatten" {
  value = [for k,v in local.x:
    [for kk, vv in v:
      [for i in vv: "${k}=${kk}=${i}"]
    ]
  ]
}

output "nestedWithFlatten" {
  value = flatten([for k,v in local.x:
                [for kk, vv in v:
                    [for i in vv: "${k}=${kk}=${i}"]
                ]
            ])
}
