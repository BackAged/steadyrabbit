package steadyrabbit_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSteadyrabbit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Steadyrabbit Suite")
}
