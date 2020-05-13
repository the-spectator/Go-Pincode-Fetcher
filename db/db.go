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
				panic(err.Error())
			}
			return c, err
		},
	}
}

func AppendToCities(conn redis.Conn, value string) error {
	_, err := conn.Do("SADD", "cities", value)
	// defer conn.Close()

	if err != nil {
		log.Fatal(err)
	}
	return err
}

func ListCities(conn redis.Conn) (cities []string, err error) {
	cities, err = redis.Strings(conn.Do("SMEMBERS", "cities"))
	// defer conn.Close()

	if err != nil {
		log.Fatal(err)
		return
	}
	return
}

func ResetCities(conn redis.Conn) error {
	// defer conn.Close()
	_, err := conn.Do("DEL", "cities")
	return err
}
