package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
)

func calcHash(key string, size int) (int, error) {
	if len(key) == 0 { // Проверяем, наличие ключа
		return 0, errors.New("KEY==0")
	}
	hash := 0
	for i := 0; i < len(key); i++ { //сумируем сумму символов
		hash += int(key[i])
	}
	return hash % size, nil // делим hash на  количество элементов таблицы
}

type Pair struct {
	key   string
	value string
}

type HashMap struct {
	table [512]*Pair
	mutex sync.Mutex
}

func (hashmap *HashMap) insert(key string, value string) error {
	hashmap.mutex.Lock()
	p := &Pair{key: key, value: value}             // Создаем новую пару
	hash, err := calcHash(key, len(hashmap.table)) // считаем hash
	if err != nil {
		hashmap.mutex.Unlock()
		return err
	}
	if hashmap.table[hash] == nil { // Если ячейка таблицы с этим хэшем пуста
		hashmap.table[hash] = p // добавляем пару в эту ячейку
		hashmap.mutex.Unlock()
		return nil
	}
	if hashmap.table[hash].key == key { //Если в ячейке уже есть пара с таким же ключом, возвращаем ошибку
		hashmap.mutex.Unlock()
		return errors.New("an element with such a key exists")
	}
	for i := (hash + 1) % len(hashmap.table); i != hash; i = (i + 1) % len(hashmap.table) { // Ищем следующую доступную ячейку для вставки
		if hashmap.table[i] == nil { // Если ячейка таблицы с этим хэшем пуста
			hashmap.table[i] = p // добавляем пару в эту ячейку
			hashmap.mutex.Unlock()
			return nil
		}
		if hashmap.table[i].key == key { //Если в ячейке уже есть пара с таким же ключом, возвращаем ошибку
			hashmap.mutex.Unlock()
			return errors.New("an element with such a key exists")
		}
	}
	hashmap.mutex.Unlock()
	return errors.New("HashMap full")
}

func (hashmap *HashMap) get(key string) (string, error) {
	hashmap.mutex.Lock()
	hash, err := calcHash(key, len(hashmap.table)) // считаем hash
	if err != nil {
		hashmap.mutex.Unlock()
		return "", err
	}
	if hashmap.table[hash] != nil && hashmap.table[hash].key == key { // Проверяем, наличие ключа по хэшу
		hashmap.mutex.Unlock()
		return hashmap.table[hash].value, nil
	}
	for i := (hash + 1) % len(hashmap.table); i != hash; i = (i + 1) % len(hashmap.table) { // перебор в случии колизии
		if hashmap.table[i] != nil && hashmap.table[i].key == key { // Проверяем, наличие ключа по хэшу
			hashmap.mutex.Unlock()
			return hashmap.table[i].value, nil
		}
	}
	hashmap.mutex.Unlock()
	return "", errors.New("not found")
}

func (hashmap *HashMap) del(key string) error {
	hashmap.mutex.Lock()
	hash, err := calcHash(key, len(hashmap.table)) // считаем hash
	if err != nil {
		hashmap.mutex.Unlock()
		return err
	}
	if hashmap.table[hash] != nil && hashmap.table[hash].key == key { // удаление по хешу
		hashmap.table[hash] = nil
		hashmap.mutex.Unlock()
		return nil
	}
	for i := (hash + 1) % len(hashmap.table); i != hash; i = (i + 1) % len(hashmap.table) { // перебор при колизии
		if hashmap.table[i] != nil && hashmap.table[i].key == key {
			hashmap.table[i] = nil
			hashmap.mutex.Unlock()
			return nil
		}
	}
	hashmap.mutex.Unlock()
	return errors.New("not found")
}

type HashSet struct {
	table [512]*Pair
	mutex sync.Mutex
}

func (hashset *HashSet) insert(key string) error {
	hashset.mutex.Lock()
	p := &Pair{key: key}                           // Создаем новую пару
	hash, err := calcHash(key, len(hashset.table)) // считаем hash
	if err != nil {
		hashset.mutex.Unlock()
		return err
	}
	if hashset.table[hash] == nil { // Если ячейка таблицы с этим хэшем пуста
		hashset.table[hash] = p // добавляем пару в эту ячейку
		hashset.mutex.Unlock()
		return nil
	}
	if hashset.table[hash].key == key { //Если в ячейке уже есть пара с таким же ключом, возвращаем ошибку
		hashset.mutex.Unlock()
		return errors.New("an element with such a key exists")
	}
	for i := (hash + 1) % len(hashset.table); i != hash; i = (i + 1) % len(hashset.table) { // Ищем следующую доступную ячейку для вставки
		if hashset.table[i] == nil { // Если ячейка таблицы с этим хэшем пуста
			hashset.table[i] = p // добавляем пару в эту ячейку
			hashset.mutex.Unlock()
			return nil
		}
		if hashset.table[i].key == key { //Если в ячейке уже есть пара с таким же ключом, возвращаем ошибку
			hashset.mutex.Unlock()
			return errors.New("an element with such a key exists")
		}
	}
	hashset.mutex.Unlock()
	return errors.New("Set full")
}

func (hashset *HashSet) search(key string) bool {
	hashset.mutex.Lock()
	hash, err := calcHash(key, len(hashset.table)) // считаем hash
	if err != nil {
		hashset.mutex.Unlock()
		return false
	}
	if hashset.table[hash] != nil && hashset.table[hash].key == key { // Проверяем, наличие ключа по хэшу
		hashset.mutex.Unlock()
		return true
	}
	for i := (hash + 1) % len(hashset.table); i != hash; i = (i + 1) % len(hashset.table) { // перебор в случае коллизии
		if hashset.table[i] != nil && hashset.table[i].key == key { // Проверяем, наличие ключа по хэшу
			hashset.mutex.Unlock()
			return true
		}
	}
	hashset.mutex.Unlock()
	return false
}

func (hashset *HashSet) delete(key string) error {
	hashset.mutex.Lock()
	hash, err := calcHash(key, len(hashset.table)) // считаем hash
	if err != nil {
		hashset.mutex.Unlock()
		return err
	}
	if hashset.table[hash] != nil && hashset.table[hash].key == key { // Проверяем, наличие ключа по хэшу
		hashset.table[hash] = nil
		hashset.mutex.Unlock()
		return nil
	}
	for i := (hash + 1) % len(hashset.table); i != hash; i = (i + 1) % len(hashset.table) { // перебор в случае коллизии
		if hashset.table[i] != nil && hashset.table[i].key == key { // Проверяем, наличие ключа по хэшу
			hashset.table[i] = nil
			hashset.mutex.Unlock()
			return nil
		}
	}
	hashset.mutex.Unlock()
	return errors.New("element not found")
}

type Node struct {
	data string
	next *Node
}

type Queue struct {
	head  *Node
	tail  *Node
	mutex sync.Mutex
}

func (queue *Queue) spush(data string) {
	queue.mutex.Lock()
	newNode := &Node{ // Создаем новый узел с заданными данными и указателем next на nil
		data: data,
		next: nil,
	}
	if queue.head == nil { // Проверяем, пуста ли очередь
		queue.head = newNode //устанавливаем указатель head на только что созданный узел
		queue.tail = newNode //устанавливаем указатель tail на только что созданный узел
	} else {
		queue.tail.next = newNode //устанавливаем указатель next последнего узла на только что созданный узел
		queue.tail = newNode      //обновление указателя на tail
	}
	queue.mutex.Unlock()
}

func (queue *Queue) spop() string {
	queue.mutex.Lock()
	if queue.head == nil { // Проверяем, пуста ли очередь
		queue.mutex.Unlock()
		return ""
	}
	data := queue.head.data      // Сохраняем значение узла
	queue.head = queue.head.next // Обновляем указатель head
	if queue.head == nil {       // Проверяем, пуста ли очередь после удаления элемента
		queue.tail = nil // обнуляем  tail
	}
	queue.mutex.Unlock()
	return data
}

func (queue *Queue) Empty1() bool { //проверка пуста ли очередь
	queue.mutex.Lock()
	queue.mutex.Unlock()
	return queue.head == nil
}

type Stack struct {
	head  *Node
	mutex sync.Mutex
}

func (stack *Stack) spush(data string) {
	stack.mutex.Lock()
	newNode := &Node{ // Создаем новый узел с заданными данными и указателем next на текущую голову стека.
		data: data,
		next: stack.head,
	}
	stack.head = newNode // Обновляем указатель head, чтобы он указывал на только что созданный узел.
	stack.mutex.Unlock()
}

func (stack *Stack) spop() string {
	stack.mutex.Lock()
	if stack.head == nil { // Проверяем, пуст ли стек
		stack.mutex.Unlock()
		return ""
	}
	data := stack.head.data      //запоминаем значение узла для вывода
	stack.head = stack.head.next //передоём указатель на предыдущий узел
	stack.mutex.Unlock()
	return data
}

func (stack *Stack) Empty2() bool { // проверяем пуст ли стек
	stack.mutex.Lock()
	stack.mutex.Unlock()
	return stack.head == nil
}

var queue Queue
var hashmap HashMap
var stack Stack
var hset HashSet

func Vibor(command string) string {
	var res string
	parts := strings.Split(command, " ")
	switch parts[0] {
	case "QPUSH":
		if len(parts) == 1 {
			res = "Невведён элемент добавления"
			return res
		}
		data := strings.Join(parts[1:], " ")
		queue.spush(data)
		res = "Элемент добавлен в очередь"
		return res
	case "QPOP":
		if queue.Empty1() {
			res = "Очередь пуста"
			return res
		}
		popData := queue.spop()
		res = fmt.Sprintf("Извлеченный элемент: %s", popData)
		return res
	case "HSET":
		if len(parts) < 3 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		value := parts[2]
		err := hashmap.insert(key, value)
		if err != nil {
			res = fmt.Sprintf("Ошибка: %s", err)
			return res
		}
		res = "Элемент добавлен в хеш-таблицу"
		return res
	case "HGET":
		if len(parts) < 2 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		value, err := hashmap.get(key)
		if err != nil {
			res = fmt.Sprintf("Error")
			return res
		} else {
			res = fmt.Sprintf(value)
			return res
		}
	case "HDEL":
		if len(parts) < 2 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		err := hashmap.del(key)
		if err != nil {
			res = fmt.Sprintf("Ошибка: %s", err)
			return res
		}
		res = "Элемент удалён из хеш-таблицы"
		return res
	case "SPUSH":
		if len(parts) == 1 {
			res = "Невведён элемент добавления"
			return res
		}
		data := strings.Join(parts[1:], " ")
		stack.spush(data)
		res = "Элемент добавлен в Стек"
		return res
	case "SPOP":
		if stack.Empty2() {
			res = "Стек пуст"
			return res
		}
		popData := stack.spop()
		res = fmt.Sprintf("Извлеченный элемент: %s", popData)
		return res
	case "SADD":
		if len(parts) < 2 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		err := hset.insert(key)
		if err != nil {
			res = fmt.Sprintf("Ошибка: %s", err)
			return res
		}
		res = "Элемент добавлен в Множество"
		return res
	case "SISMEMBER":
		if len(parts) < 2 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		if hset.search(key) {
			res = fmt.Sprintf("Элемент %s присутствует во множестве", key)
		} else {
			res = fmt.Sprintf("Элемент %s отсутствует во множестве", key)
		}
		return res
	case "SREM":
		if len(parts) < 2 {
			res = "Неверный формат команды"
			return res
		}
		key := parts[1]
		err := hset.delete(key)
		if err != nil {
			res = fmt.Sprintf("Ошибка: %s", err)
			return res
		} else {
			res = fmt.Sprintf("Элемент %s удалён", key)
			return res
		}
	default:
		res = "Неверная команда"
		return res
	}
}

func main() {
	listener, err := net.Listen("tcp", ":6379") //подключение к порту 6379
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()
	fmt.Println("Сервер слушает...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			conn.Close()
			continue
		}
		go handleConnection(conn) // запускаем горутину для обработки запроса
	}
}

// обработка подключения
func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		// считываем полученные в запросе данные
		input := make([]byte, (1024 * 4))
		n, _ := conn.Read(input)
		command := string(input[0:n])
		// вызываем функцию Vibor для обработки команды
		response := Vibor(command)
		// выводим на консоль сервера диагностическую информацию
		fmt.Println(command, "-", response)
		// отправляем данные клиенту
		conn.Write([]byte(response))
	}
}
