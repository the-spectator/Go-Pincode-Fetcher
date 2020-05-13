package db

import (
	// Import the redigo/redis package.

	"log"

	"github.com/gomodule/redigo/redis"
)

func NewPool() *redis.Pool {
	return &redis.Pool{
		// Maximum number of idle connections in the pool.
		MaxIdle: 1500,
		// max number of connections
		MaxActive: 12000,
		// Dial is an application supplied function for creating and
		// configuring a connection.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				log.Println("Pool dial panic")
				panic(err.Error())
			}
			return c, err
		},
	}
}

func Exists(conn redis.Conn, listKey string, value string) bool {
	val, err := redis.Bool(conn.Do("SISMEMBER", listKey, value))
	if err != nil {
		log.Fatalf("Inside db.exists with error %v", err)
		return val
	}
	return val
}

func PopList(conn redis.Conn, listKey string, count int) (list []string, err error) {
	list, err = redis.Strings(conn.Do("SPOP", listKey, count))
	if err != nil {
		log.Fatalf("Inside db.popList with error %v", err)
		return
	}
	return
}

func AppendToCities(conn redis.Conn, value string) error {
	err := AppendToList(conn, "cities", value)
	return err
}

func AppendToPincodes(conn redis.Conn, value string) error {
	err := AppendToList(conn, "pincodes", value)
	return err
}

func AppendToList(conn redis.Conn, listKey string, value string) (err error) {
	_, err = conn.Do("SADD", listKey, value)

	if err != nil {
		log.Fatalf("Inside Append to List error %v", err)
	}
	return err
}

func LengthOfList(conn redis.Conn, listKey string) (length int, err error) {
	length, err = redis.Int(conn.Do("SCARD", listKey))

	if err != nil {
		log.Fatalf("Inside Length of the List with error %v", err)
		return
	}
	return
}

func ListCities(conn redis.Conn) (cities []string, err error) {
	cities, err = redis.Strings(conn.Do("SMEMBERS", "cities"))
	// defer conn.Close()

	if err != nil {
		log.Fatalf("Inside db.ListCities with error %v", err)
		return
	}
	return
}

func ResetList(conn redis.Conn, listKey string) error {
	// defer conn.Close()
	_, err := conn.Do("DEL", listKey)
	return err
}

func IncrementAPICounter(conn redis.Conn) {
	_, _ = conn.Do("INCR", "apiCount")
}

func ResetAPICounter(conn redis.Conn) {
	_, _ = conn.Do("SET", "apiCount", 0)
}
