package service

type Repository interface {
	Put(key, value string, expirationTime int64) error
	Get(key string) (string, error)
	Delete(key string)
}

type Service struct {
	repository Repository
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository}
}

func (s *Service) Get(key string) (string, error) {
	return s.repository.Get(key)
}

func (s *Service) Put(key, value string, expirationTime int64) error {
	return s.repository.Put(key, value, expirationTime)
}

func (s *Service) Delete(key string) {
	s.repository.Delete(key)
}
