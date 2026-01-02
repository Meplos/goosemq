package data

type PullResponse struct {
	Items []TopicMessage `json:"items"`
}

type AckStatus string

const (
	AckOk AckStatus = "OK"
	AckKo AckStatus = "KO"
)

type AckResponse struct {
	Status AckStatus `json:"status"`
	Error  string    `json:"error,omitempty"`
}
