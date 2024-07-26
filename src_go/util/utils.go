package util

import (
	"fmt"

	"github.com/common-nighthawk/go-figure"
)

func PrintSpace() {
	fmt.Println("")
}

func PrintBanner() {
	myFigure := figure.NewFigure("Architects That Code", "", true)
	myFigure.Print()
	PrintSpace()
}
