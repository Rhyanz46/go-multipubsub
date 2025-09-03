package multi_pubsub

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type Strategy int

const (
	JsonKeyStrategy Strategy = iota + 5599
	CheckStringStrategy
	CheckStringSensitiveCaseStrategy
)

func randomString(total int) string {
	var result string
	uuidRandom, _ := uuid.NewRandom()
	for len(result) < total {
		result += strings.ReplaceAll(uuidRandom.String(), "-", "")
	}
	result = result[0:total]
	return result
}

type Condition struct {
	Key  string
	Data interface{}
}

type BaseCallback interface {
	SetTopic(topic string) BaseCallback
	Subscribe() BaseCallback
	Unsubscribe()
	checkCondition()
	OnError(fun func(data []byte)) BaseCallback
	OnEveryEvent(fun func(data []byte)) BaseCallback
	OnEvent(conditions []Condition, strategy Strategy, fun func(data []byte)) BaseCallback
}

type baseCallback[T any] struct {
	httpData http.Response
	broker   T
	message  *[]byte

	conditionsFunc     map[string]func(data []byte)
	conditionsStrategy map[string]Strategy
	conditions         map[string][]Condition

	onErrorFunc  *func(data []byte)
	onEveryEvent *func(data []byte)
	state        interface{}
	topic        string
}

func (dnr *baseCallback[any]) Unsubscribe() {
	panic("implement me")
}

func NewCallback[T any](broker T, state interface{}, httpData http.Response) BaseCallback {
	return &baseCallback[any]{
		conditionsStrategy: map[string]Strategy{},
		conditionsFunc:     map[string]func(data []byte){},
		conditions:         map[string][]Condition{},
		broker:             broker,
		state:              state,
		httpData:           httpData,
	}
}

func (dnr *baseCallback[any]) SetTopic(topic string) BaseCallback {
	if dnr.topic != "" {
		panic("cant set topic twice")
	}
	dnr.topic = topic
	return dnr
}

func (dnr *baseCallback[any]) Subscribe() BaseCallback {
	panic("implement me")
}

func (dnr *baseCallback[any]) checkCondition() {
	msg := *dnr.message
	for key, strategy := range dnr.conditionsStrategy {
		conditions := dnr.conditions[key]
		fun := dnr.conditionsFunc[key]
		onErrFun := *dnr.onErrorFunc
		payloadResponseStr := string(msg)

		switch strategy {
		case JsonKeyStrategy:
			var notOk bool
			data := make(map[string]interface{})
			payloadResponseStr = strings.Replace(payloadResponseStr, "'", "\"", -1)
			err := json.Unmarshal([]byte(payloadResponseStr), &data)
			if err != nil {
				onErrFun(msg)
				return
			}
			for _, condition := range conditions {
				if data[condition.Key] != condition.Data {
					notOk = true
					break
				}
			}
			if !notOk {
				fun(msg)
				return
			}
		case CheckStringStrategy:
			var notOk bool
			for _, condition := range conditions {
				if !strings.Contains(strings.ToLower(payloadResponseStr), strings.ToLower(condition.Data.(string))) {
					notOk = true
					break
				}
			}
			if !notOk {
				fun(msg)
				return
			}
		case CheckStringSensitiveCaseStrategy:
			var notOk bool
			for _, condition := range conditions {
				if !strings.Contains(payloadResponseStr, condition.Data.(string)) {
					notOk = true
				}
			}
			if !notOk {
				fun(msg)
				return
			}
		}

		if dnr.onEveryEvent != nil {
			onEveryEvent := *dnr.onEveryEvent
			onEveryEvent(msg)
		}
	}
}

func (dnr *baseCallback[any]) OnError(fun func(data []byte)) BaseCallback {
	if dnr.onErrorFunc != nil {
		panic("you cant set on error twice")
	}
	dnr.onErrorFunc = &fun
	return dnr
}

func (dnr *baseCallback[any]) OnEveryEvent(fun func(data []byte)) BaseCallback {
	if dnr.onEveryEvent != nil {
		panic("you cant set on error twice")
	}
	dnr.onEveryEvent = &fun
	return dnr
}

func (dnr *baseCallback[any]) OnEvent(conditions []Condition, strategy Strategy, fun func(data []byte)) BaseCallback {
	if len(conditions) == 0 {
		panic("you need to add Condition")
	}
	if strategy == JsonKeyStrategy && conditions[0].Key == "" {
		panic("you need to set the first Condition")
	}
	if conditions[0].Data == nil {
		panic("you need to set the second Condition")
	}
	if strategy == CheckStringStrategy || strategy == CheckStringSensitiveCaseStrategy {
		_, ok := conditions[0].Data.(string)
		if !ok {
			panic("if your strategy is on string check you should put data with string")
		}
	}
	key := randomString(10)
	dnr.conditions[key] = conditions
	dnr.conditionsFunc[key] = fun
	dnr.conditionsStrategy[key] = strategy
	return dnr
}
