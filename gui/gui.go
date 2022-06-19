package main

import (
	"log"
	"fyne.io/fyne/v2"
	"image/color"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"fyne.io/fyne/v2/canvas"
	"kafka-go-streaming/producer"
)

func main() {
	myApp := app.New()
	myWindow := myApp.NewWindow("Entry Widget")
	myWindow.Resize(fyne.NewSize(800, 300))
	title := canvas.NewText("Add your important links", color.White)
	title.TextStyle = fyne.TextStyle{
		Bold: true,
	}
	title.Alignment = fyne.TextAlignCenter
	title.TextSize = 24

	link := widget.NewEntry()
	link.SetPlaceHolder("Enter link...")
	description := widget.NewEntry()
	description.SetPlaceHolder("Enter description...")
	topic := widget.NewEntry()
	topic.SetPlaceHolder("Enter topic...")


	content := container.NewVBox(title, link,description, topic, widget.NewButton("Add", func() {
		// log.Println("Content was:", link.Text)
		words := producer.Link{
			Link:        link.Text,
			Description: description.Text,
			Topic:   	 topic.Text,
		}
		log.Println("Content was:", words)
	}))

	myWindow.SetContent(content)
	myWindow.ShowAndRun()
}