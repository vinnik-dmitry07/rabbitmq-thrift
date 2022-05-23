enum Operation {
  ADD = 1,
  SUBTRACT = 2,
  MULTIPLY = 3,
  DIVIDE = 4
}

struct Work {
  1: i32 num1,
  2: i32 num2,
  3: Operation op,
  4: optional string comment
}

exception InvalidOperation {
  1: i32 whatOp,
  2: string why
}

union Result {
  1: i32 val,
  2: InvalidOperation ouch
}
