package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type User struct {
	Username string
	Password string
	Roles    []string
}

type Token struct {
	Value     string
	Username  string
	Roles     []string
	ExpiresAt time.Time
}

type Manager struct {
	users    map[string]*User
	tokens   map[string]*Token
	mu       sync.RWMutex
	tokenTTL time.Duration
}

func NewManager(tokenTTL time.Duration) *Manager {
	return &Manager{
		users:    make(map[string]*User),
		tokens:   make(map[string]*Token),
		tokenTTL: tokenTTL,
	}
}

func (m *Manager) CreateUser(username, password string, roles []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.users[username]; exists {
		return fmt.Errorf("user %s already exists", username)
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %v", err)
	}

	m.users[username] = &User{
		Username: username,
		Password: string(hashedPassword),
		Roles:    roles,
	}

	return nil
}

func (m *Manager) Authenticate(username, password string) (*Token, error) {
	m.mu.RLock()
	user, exists := m.users[username]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("user %s not found", username)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return nil, fmt.Errorf("invalid password")
	}

	tokenValue, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %v", err)
	}

	token := &Token{
		Value:     tokenValue,
		Username:  username,
		Roles:     user.Roles,
		ExpiresAt: time.Now().Add(m.tokenTTL),
	}

	m.mu.Lock()
	m.tokens[tokenValue] = token
	m.mu.Unlock()

	return token, nil
}

func (m *Manager) ValidateToken(tokenValue string) (*Token, error) {
	m.mu.RLock()
	token, exists := m.tokens[tokenValue]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("invalid token")
	}

	if time.Now().After(token.ExpiresAt) {
		m.mu.Lock()
		delete(m.tokens, tokenValue)
		m.mu.Unlock()
		return nil, fmt.Errorf("token expired")
	}

	return token, nil
}

func (m *Manager) HasRole(tokenValue, role string) bool {
	token, err := m.ValidateToken(tokenValue)
	if err != nil {
		return false
	}

	for _, r := range token.Roles {
		if r == role {
			return true
		}
	}

	return false
}

func (m *Manager) RevokeToken(tokenValue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tokens[tokenValue]; !exists {
		return fmt.Errorf("token not found")
	}

	delete(m.tokens, tokenValue)
	return nil
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func constantTimeCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
