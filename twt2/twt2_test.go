package twt2

import (
//  "fmt"
  "testing"
)

func TestTf(t *testing.T) {
  res := tf(1)
  if res != 2 {
    t.Fatalf("tf(1) = %d instead of 2", res)
  }
}
