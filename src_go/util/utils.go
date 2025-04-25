package util

import (
	"fmt"

	"github.com/common-nighthawk/go-figure"
)

func PrintSpace() {
	fmt.Println("")
}

func PrintBanner() {
	myFigure := figure.NewColorFigure("Architects That Code", "", "blue", false)
	//myFigure.Scroll(800, 100, "left")
	myFigure.Print()
	PrintSpace()
}
