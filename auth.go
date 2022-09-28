package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gomodule/redigo/redis"
)

type redisResponse struct {
	UserID string `json:"value"`
}

func NewRedisPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 20,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}

			return c, nil
		},
	}
}

func GetAuthToken(redis_pool *redis.Pool, r *http.Request) (string, error) {
	client := redis_pool.Get()
	defer client.Close()

	cookie, err := r.Cookie("api_key")
	var token string

	switch err {
	case http.ErrNoCookie:
		token = r.Header.Get("Authorization")
	case nil:
		cookie_str := cookie.String()
		if cookie_str == "" {
			return "", errors.New("empty or no cookie recieved")
		}

		// slicing beacause the returned cookie string is of format "set-cookie=token"
		token = cookie_str[8:]
	}

	if token == "" {
		return "", errors.New("empty or no authorization token provided")
	}

	res, err := redis.Bytes(client.Do("GET", "login.token."+token))
	if err != nil {
		return "", err
	}

	var user redisResponse
	err = json.Unmarshal(res, &user)
	if err != nil {
		return "", err
	}

	return user.UserID, nil
}
