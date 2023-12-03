package main

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

func conect(source string) string { // функция подключения к серверу СУБД
	conn, err := net.Dial("tcp", ":6379") // Подключение к серверу на порту 6379
	if err != nil {
		fmt.Println(err)
		return "Error"
	}
	defer conn.Close() //разрыв соединения при прекрощении функции main
	for {
		// отправляем сообщение серверу
		if n, err := conn.Write([]byte(source)); n == 0 || err != nil {
			return "Errorr"
		}
		// получаем ответ
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			return "Error"
		}
		return string(buff[0:n])
	}
}

func generateShortKey(input string) string { // hash функция для генерации сокращённой ссылки
	var hash uint32
	for _, char := range input {
		hash += uint32(char)
	}
	return fmt.Sprintf("%x", hash)
}

func handleForm(w http.ResponseWriter, r *http.Request) { //Начальная страница
	if r.Method == http.MethodPost { // если метод запроса POST
		http.Redirect(w, r, "/shorten", http.StatusSeeOther)
		return
	}

	// если метод запроса не POST запускается HTML-код
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, `
	<!DOCTYPE html>
	<html>
	<head>
		<title>URL Shortener</title>
	</head>
	<body>
		<h2>URL Shortener</h2>
		<form method="post" action="/shorten">
			<input type="url" name="url" placeholder="Enter a URL" required>
			<input type="submit" value="Shorten">
		</form>
	</body>
	</html>
	`)
}

func handleShorten(w http.ResponseWriter, r *http.Request) { //Сокращение ссылки
	if r.Method != http.MethodPost { //проверка, что возможен метод Post
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	originalURL := r.FormValue("url")
	if originalURL == "" { // проверка что ссылка не пуста
		http.Error(w, "URL parameter is missing", http.StatusBadRequest)
		return
	}

	// Создание сокращённой ссылки и запись её в хеш таблицу
	shortKey := generateShortKey(originalURL)
	source := "HSET" + " " + shortKey + " " + originalURL
	err := conect(source)
	if err == "Error" {
		http.Error(w, "Server not found", http.StatusNotFound)
		return
	}
	shortenedURL := fmt.Sprintf("http://localhost:3030/short/%s", shortKey)

	// HTML-код
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, `
	<!DOCTYPE html>
	<html>
	<head>
		<title>URL Shortener</title>
	</head>
	<body>
		<h2>URL Shortener</h2>
		<p>Original URL: `, originalURL, `</p>
		<p>Shortened URL: <a href="`, shortenedURL, `">`, shortenedURL, `</a></p>
	</body>
	</html>
	`)
}

func handleRedirect(w http.ResponseWriter, r *http.Request) {
	shortKey := strings.TrimPrefix(r.URL.Path, "/short/") // Извлекаем короткий ключ из URL пути, удаляя префикс "/short/"
	if shortKey == "" {                                   //Проверка на присутсвие сокращения
		http.Error(w, "There is no abbreviated key", http.StatusBadRequest)
		return
	}

	// Извлечение исходного URL-адреса из хеш-таблицы
	source := "HGET" + " " + shortKey
	originalURL := conect(source)
	if originalURL == "Error" { //проверка есть ли оригинальная ссылка в хеш-таблице
		http.Error(w, "Shortened key not found", http.StatusNotFound)
		return
	}
	http.Redirect(w, r, originalURL, http.StatusMovedPermanently) // Перенаправление пользователя на изначальный сайт
}

func main() {
	http.HandleFunc("/", handleForm)
	http.HandleFunc("/shorten", handleShorten)
	http.HandleFunc("/short/", handleRedirect)
	fmt.Println("URL Shortener is running on :3030")
	http.ListenAndServe(":3030", nil)
}
