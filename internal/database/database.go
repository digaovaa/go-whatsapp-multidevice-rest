package database

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dimaskiddo/go-whatsapp-multidevice-rest/pkg/log"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	dbInstance *gorm.DB
	dbConnStr  string
	dbOnce     sync.Once
)

type Service interface {
	CreateUser(user *User) (int, error)
	UpdateUser(user *User) error
	DeleteUser(id int) error
	SetQrcode(id int, qrcode string, instance string) error
	SetWebhook(id int, webhook string) error
	SetConnected(id int) error
	SetDisconnected(id int) error
	SetJid(id int, jid string) error
	SetEvents(id int, events string) error
	GetUserById(id int) (*User, error)
	GetUserByToken(token string) (*User, error)
	// ListConnectedUsers retorna todos os usuários conectados
	ListConnectedUsers() ([]*User, error)
	// SetPairingCode salva o código de pairing do usuário
	SetPairingCode(id int, pairingCode string, instance string) error
	// SetCountMsg incrementa o contador de mensagens diárias do usuário
	SetCountMsg(id uint, typeMsg string) error
	CheckAndSetUserOnline() error

	GetCompanyByToken(token string) (*Company, error)
	CountConnectedUsers(instance string) (int, error)
	ListAllUsersCompany(companyId int, instance string) ([]*User, error)
}

type User struct {
	gorm.Model
	ID               uint    `gorm:"primaryKey"`
	Name             string  `gorm:"type:text;not null;index"`
	Token            string  `gorm:"type:text;not null;index"`
	Webhook          string  `gorm:"type:text;not null;default:''"`
	Jid              string  `gorm:"type:text;not null;default:''"`
	Qrcode           string  `gorm:"type:text;not null;default:''"`
	Connected        int     `gorm:"type:integer;index"`
	Expiration       int     `gorm:"type:integer"`
	Events           string  `gorm:"type:text;not null;default:'All'"`
	PairingCode      string  `gorm:"type:text;not null;default:''"`
	Instance         string  `gorm:"type:text;not null;default:''"`
	CountTextMsg     int     `gorm:"type:integer;default:0"`
	CountImageMsg    int     `gorm:"type:integer;default:0"`
	CountVoiceMsg    int     `gorm:"type:integer;default:0"`
	CountVideoMsg    int     `gorm:"type:integer;default:0"`
	CountStickerMsg  int     `gorm:"type:integer;default:0"`
	CountLocationMsg int     `gorm:"type:integer;default:0"`
	CountContactMsg  int     `gorm:"type:integer;default:0"`
	CountDocumentMsg int     `gorm:"type:integer;default:0"`
	CompanyId        int     `gorm:"default:null"`
	Company          Company `gorm:"foreignKey:CompanyId;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"` // Relacionamento correto
	WhatsappId       int     `gorm:"type:integer;default:null"`
}

type UserHistory struct {
	gorm.Model
	ID               uint       `gorm:"primaryKey"`
	UserID           uint       `gorm:"not null;index"`
	User             *User      `gorm:"foreignKey:UserID"`
	Date             time.Time  `gorm:"type:timestamp;index"`
	CountTextMsg     int        `gorm:"type:integer;default:0"`
	CountImageMsg    int        `gorm:"type:integer;default:0"`
	CountVoiceMsg    int        `gorm:"type:integer;default:0"`
	CountVideoMsg    int        `gorm:"type:integer;default:0"`
	CountStickerMsg  int        `gorm:"type:integer;default:0"`
	CountLocationMsg int        `gorm:"type:integer;default:0"`
	CountContactMsg  int        `gorm:"type:integer;default:0"`
	CountDocumentMsg int        `gorm:"type:integer;default:0"`
	IsOnline         bool       `gorm:"type:boolean;default:false"`
	DisconnectedAt   *time.Time `gorm:"type:timestamp;default:null"`
	ConnectedAt      *time.Time `gorm:"type:timestamp;default:null"`
}

type Company struct {
	gorm.Model
	ID                  int        `gorm:"primaryKey"`
	Name                string     `gorm:"type:text;not null;index"`
	Token               string     `gorm:"type:text;not null;index"`
	ConnectionsLimit    int        `gorm:"type:integer;default:10"`
	ConnectionsInstance int        `gorm:"type:integer;default:200"`
	DateLimit           *time.Time `gorm:"type:timestamp;default:null"`
	RedisUri            string     `gorm:"type:text;not null;default:''"`
}

type service struct {
	db *gorm.DB
}

func startMysql() (*gorm.DB, error) {
	// log.Print(nil).Info("Starting mysql")

	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbUser, dbPass, dbHost, dbPort, dbName)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		log.Print(nil).Error("Could not open/create " + dsn)
		return nil, err
	}

	return db, nil
}

// startPostgres initializes the database connection if it hasn't been already
func startPostgres() (*gorm.DB, error) {
	dbOnce.Do(func() {
		// log.Print(nil).Info("Starting postgres")

		dbConnStr = os.Getenv("WHATSAPP_DATASTORE_URI")
		var err error
		dbInstance, err = gorm.Open(postgres.Open(dbConnStr), &gorm.Config{
			PrepareStmt: true, // Prepara as declarações
		})
		if err != nil {
			log.Print(nil).Error("Could not open/create " + dbConnStr)
			return
		}
		// Configurando o pool de conexões
		sqlDB, err := dbInstance.DB()
		if err != nil {
			log.Print(nil).Error("Could not get DB from gorm.DB")
			return
		}

		// Definindo configurações de pool de conexão
		sqlDB.SetMaxIdleConns(15)  // Número máximo de conexões inativas
		sqlDB.SetMaxOpenConns(300) // Número máximo de conexões abertas
		sqlDB.SetConnMaxLifetime(time.Duration(30000) * time.Millisecond)
		sqlDB.SetConnMaxIdleTime(time.Duration(600000) * time.Millisecond)

		log.Print(nil).Info("Connected to database")
	})

	if dbInstance == nil {
		return nil, fmt.Errorf("could not establish database connection")
	}

	return dbInstance, nil
}

func NewService(driver string) (Service, error) {
	var err error
	var db *gorm.DB

	switch driver {
	case "mysql":
		db, err = startMysql()
	case "postgres":
		db, err = startPostgres()
	default:
		return nil, fmt.Errorf("driver not supported")
	}

	log.Print(nil).Info("Migrating database")
	db.AutoMigrate(&Company{}, &User{}, &UserHistory{})

	if err != nil {
		return nil, err
	}

	s := &service{db: db}

	return s, nil
}

func (s *service) CreateUser(user *User) (int, error) {

	result := s.db.Create(user)

	if result.Error != nil {
		log.Print(nil).Error("Could not create user", result.Error)

		return 0, result.Error
	}

	return int(user.ID), nil
}

func (s *service) UpdateUser(user *User) error {

	result := s.db.Save(user)

	if result.Error != nil {
		log.Print(nil).Error("Could not update user", result.Error)

		return result.Error
	}

	return nil
}

func (s *service) SetQrcode(id int, qrcode string, instance string) error {
	// log.Info().Msgf("Attempting to set QR code for user %d with instance %s", id, instance)
	result := s.db.Model(&User{}).Where("id = ?", id).Where("instance = ?", instance).Update("qrcode", qrcode)
	if result.Error != nil {
		log.Print(nil).Error("Could not set qrcode for user", result.Error)
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Print(nil).Warnf("No rows affected when setting QR code for user %d with instance %s", id, instance)
		return fmt.Errorf("no rows affected")
	}

	// log.Info().Msgf("Successfully set QR code for user %d with instance %s", id, instance)
	return nil
}

func (s *service) SetWebhook(id int, webhook string) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Update("webhook", webhook).Error

	if err != nil {
		log.Print(nil).Error("Could not set webhook", err)

		return err
	}

	return nil
}

func (s *service) SetConnected(id int) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Update("connected", 1).Error

	if err != nil {
		log.Print(nil).Error("Could not set user as connected", err)

		return err
	}

	return nil
}

func (s *service) SetDisconnected(id int) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Update("connected", 0).Error

	if err != nil {
		log.Print(nil).Error("Could not set user as disconnected", err)

		return err
	}

	return nil
}

func (s *service) SetJid(id int, jid string) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Update("jid", jid).Error

	if err != nil {
		log.Print(nil).Error("Could not set jid", err)

		return err
	}

	return nil
}

func (s *service) SetEvents(id int, events string) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Update("events", events).Error

	if err != nil {
		log.Print(nil).Error("Could not set events", err)

		return err
	}

	return nil
}

func (s *service) SetPairingCode(id int, pairingCode string, instance string) error {

	err := s.db.Model(&User{}).Where("id = ?", id).Where("instance = ?", instance).Update("pairing_code", pairingCode).Error

	if err != nil {
		log.Print(nil).Error("Could not set pairing code", err)

		return err
	}

	return nil
}

// SetCountMsg incrementa o contador de mensagens diárias do usuário
func (s *service) SetCountMsg(userID uint, typeMsg string) error {
	// Definir a data atual
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	// Iniciar uma transação
	tx := s.db.Begin()
	if tx.Error != nil {
		log.Print(nil).Error("Could not start transaction", tx.Error)
		return tx.Error
	}

	// Defer para garantir que a transação seja commitada ou rollback
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if tx.Error != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Encontrar ou criar o registro para o dia atual
	var userHistory UserHistory
	err := tx.Where("user_id = ? AND date = ?", userID, today).FirstOrCreate(&userHistory, UserHistory{
		UserID:         userID,
		Date:           today,
		DisconnectedAt: nil,
		ConnectedAt:    nil,
		IsOnline:       false,
	}).Error
	if err != nil {
		fmt.Println("Erro ao buscar ou criar UserHistory:", err)
		tx.Rollback()
		return err
	}

	// Atualizar os campos baseados no tipo de mensagem
	switch typeMsg {
	case "disconnected":
		disconnectedAt := time.Now()
		err = tx.Model(&userHistory).Updates(map[string]interface{}{
			"disconnected_at": &disconnectedAt,
			"is_online":       false,
		}).Error
	case "online":
		connectedAt := time.Now()
		err = tx.Model(&userHistory).Updates(map[string]interface{}{
			"connected_at": &connectedAt,
			"is_online":    true,
		}).Error
	default:
		column := fmt.Sprintf("count_%s_msg", typeMsg)
		err = tx.Model(&userHistory).Update(column, gorm.Expr(fmt.Sprintf("%s + ?", column), 1)).Error
	}

	if err != nil {
		fmt.Println("Erro ao atualizar UserHistory:", err)
		tx.Rollback()
		return err
	}

	return nil
}

func (s *service) CheckAndSetUserOnline() error {
	var users []User
	if err := s.db.Where("connected = ?", 1).Find(&users).Error; err != nil {
		fmt.Println("Erro ao buscar usuários conectados:", err)
		return err
	}

	for _, user := range users {
		if err := s.SetCountMsg(user.ID, "online"); err != nil {
			fmt.Printf("Erro ao chamar SetCountMsg para o usuário %d: %v\n", user.ID, err)
			// Aqui você pode decidir se quer continuar o loop ou parar em caso de erro
			// return err
		}
	}

	return nil
}

func (s *service) GetUserById(id int) (*User, error) {
	var user User

	err := s.db.Where("id = ?", id).First(&user).Error

	if err != nil {
		log.Print(nil).Error("Could not get user", err)
		return nil, err
	}

	return &user, nil
}

func (s *service) GetUserByToken(token string) (*User, error) {
	var user User

	err := s.db.Where("token = ?", token).First(&user).Error

	if err != nil {
		log.Print(nil).Error("Could not get user", err)
		return nil, err
	}

	return &user, nil
}

func (s *service) GetCompanyByToken(token string) (*Company, error) {
	var company Company

	err := s.db.Where("token = ?", token).First(&company).Error

	if err != nil {
		log.Print(nil).Error("Could not get company", err)
		return nil, err
	}

	return &company, nil
}

func (s *service) ListConnectedUsers() ([]*User, error) {
	var users []*User
	instance := os.Getenv("INSTANCE")

	if instance == "" {
		panic("INSTANCE is not set")
	}

	err := s.db.Where("connected = ? AND instance = ?", 1, instance).Find(&users).Error

	if err != nil {
		log.Print(nil).Error("Could not list users", err)

		return nil, err
	}

	return users, nil
}

func (s *service) ListAllUsersCompany(companyId int, instance string) ([]*User, error) {
	var users []*User

	err := s.db.Where("company_id = ?", companyId).Where("instance = ?", instance).Where("deleted_at IS NULL").Order("connected DESC").Order("id ASC").Find(&users).Error

	if err != nil {
		log.Print(nil).Error("Could not list users", err)

		return nil, err
	}

	return users, nil
}

func (s *service) DeleteUser(id int) error {

	err := s.db.Delete(&User{}, id).Error

	if err != nil {
		log.Print(nil).Error("Could not delete user", err)

		return err
	}

	return nil
}

// Conta usuários conectados para uma `instancia` específica
func (s *service) CountConnectedUsers(instance string) (int, error) {
	var count int64
	err := s.db.Table("users").Where("instance = ? AND connected = ? and deleted_at IS NULL", instance, 1).Count(&count).Error
	return int(count), err
}
