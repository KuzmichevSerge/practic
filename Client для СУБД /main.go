package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", ":6379") // Подключение к серверу на порту 6379
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close() //разрыв соединения при прекрощении функции main
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Введите команду HSET, HDEL, HGET, QPUSH, QPOP, SPUSH, SPOP, SADD, SISMEMBER, SREM: ")

		source, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Некорректный ввод", err)
			continue
		}
		source = strings.TrimSpace(source)
		// отправляем сообщение серверу
		if n, err := conn.Write([]byte(source)); n == 0 || err != nil {
			fmt.Println(err)
			return
		}
		// получаем ответ
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			break
		}
		fmt.Print(string(buff[0:n])) // Вывод ответа от сервера на консоль
		fmt.Println()
	}
}
