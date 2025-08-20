package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Product struct {
	ID          interface{} `bson:"_id,omitempty"`
	Name        string      `bson:"name"`
	Price       float64     `bson:"price"`
	Category    string      `bson:"category"`
	Status      string      `bson:"status"`
	Description string      `bson:"description"`
	SKU         string      `bson:"sku"`
	Stock       int         `bson:"stock"`
	CreatedAt   time.Time   `bson:"createdAt"`
	UpdatedAt   time.Time   `bson:"updatedAt"`
}

type User struct {
	ID        interface{} `bson:"_id,omitempty"`
	Name      string      `bson:"name"`
	Email     string      `bson:"email"`
	Age       int         `bson:"age"`
	City      string      `bson:"city"`
	Country   string      `bson:"country"`
	Status    string      `bson:"status"`
	CreatedAt time.Time   `bson:"createdAt"`
	UpdatedAt time.Time   `bson:"updatedAt"`
}

type Order struct {
	ID         interface{} `bson:"_id,omitempty"`
	UserID     string      `bson:"userId"`
	ProductIDs []string    `bson:"productIds"`
	Total      float64     `bson:"total"`
	Status     string      `bson:"status"`
	OrderDate  time.Time   `bson:"orderDate"`
	CreatedAt  time.Time   `bson:"createdAt"`
	UpdatedAt  time.Time   `bson:"updatedAt"`
}

var (
	categories = []string{"Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Automotive", "Toys", "Food", "Health"}
	statuses   = []string{"active", "inactive", "pending", "discontinued"}
	cities     = []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	countries  = []string{"USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil", "India", "China"}
	userStatuses = []string{"active", "inactive", "suspended", "pending"}
	orderStatuses = []string{"pending", "processing", "shipped", "delivered", "cancelled"}
)

func main() {
	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb+srv://admin:IdZcKnNvmWqea13k@proptuity-dev.mgzig.mongodb.net"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	db := client.Database("sync-test-db")

	// Clear existing collections
	fmt.Println("Clearing existing collections...")
	db.Collection("products").Drop(context.TODO())
	db.Collection("users").Drop(context.TODO())
	db.Collection("orders").Drop(context.TODO())

	// Generate data
	fmt.Println("Starting data generation...")
	start := time.Now()

	// Generate 800,000 products (80% of total)
	fmt.Println("Generating 800,000 products...")
	generateProducts(db, 800000)

	// Generate 150,000 users (15% of total)
	fmt.Println("Generating 150,000 users...")
	generateUsers(db, 150000)

	// Generate 50,000 orders (5% of total)
	fmt.Println("Generating 50,000 orders...")
	generateOrders(db, 50000)

	duration := time.Since(start)
	fmt.Printf("Data generation completed in %v\n", duration)
	fmt.Println("Total documents: 1,000,000")

	// Print collection counts
	printCollectionCounts(db)
}

func generateProducts(db *mongo.Database, count int) {
	coll := db.Collection("products")
	batchSize := 1000
	batches := count / batchSize

	for batch := 0; batch < batches; batch++ {
		var products []interface{}
		for i := 0; i < batchSize; i++ {
			product := Product{
				Name:        fmt.Sprintf("Product %d", batch*batchSize+i+1),
				Price:       float64(rand.Intn(1000)) + rand.Float64(),
				Category:    categories[rand.Intn(len(categories))],
				Status:      statuses[rand.Intn(len(statuses))],
				Description: fmt.Sprintf("Description for product %d with detailed information", batch*batchSize+i+1),
				SKU:         fmt.Sprintf("SKU-%06d", batch*batchSize+i+1),
				Stock:       rand.Intn(1000),
				CreatedAt:   time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour),
				UpdatedAt:   time.Now(),
			}
			products = append(products, product)
		}

		_, err := coll.InsertMany(context.TODO(), products)
		if err != nil {
			log.Printf("Error inserting product batch %d: %v", batch, err)
			continue
		}

		if (batch+1)%10 == 0 {
			fmt.Printf("Inserted %d product batches (%d products)\n", batch+1, (batch+1)*batchSize)
		}
	}
}

func generateUsers(db *mongo.Database, count int) {
	coll := db.Collection("users")
	batchSize := 1000
	batches := count / batchSize

	for batch := 0; batch < batches; batch++ {
		var users []interface{}
		for i := 0; i < batchSize; i++ {
			user := User{
				Name:      fmt.Sprintf("User %d", batch*batchSize+i+1),
				Email:     fmt.Sprintf("user%d@example.com", batch*batchSize+i+1),
				Age:       rand.Intn(70) + 18,
				City:      cities[rand.Intn(len(cities))],
				Country:   countries[rand.Intn(len(countries))],
				Status:    userStatuses[rand.Intn(len(userStatuses))],
				CreatedAt: time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour),
				UpdatedAt: time.Now(),
			}
			users = append(users, user)
		}

		_, err := coll.InsertMany(context.TODO(), users)
		if err != nil {
			log.Printf("Error inserting user batch %d: %v", batch, err)
			continue
		}

		if (batch+1)%5 == 0 {
			fmt.Printf("Inserted %d user batches (%d users)\n", batch+1, (batch+1)*batchSize)
		}
	}
}

func generateOrders(db *mongo.Database, count int) {
	coll := db.Collection("orders")
	batchSize := 1000
	batches := count / batchSize

	for batch := 0; batch < batches; batch++ {
		var orders []interface{}
		for i := 0; i < batchSize; i++ {
			productCount := rand.Intn(5) + 1
			productIDs := make([]string, productCount)
			for j := 0; j < productCount; j++ {
				productIDs[j] = fmt.Sprintf("product-%d", rand.Intn(800000)+1)
			}

			order := Order{
				UserID:     fmt.Sprintf("user-%d", rand.Intn(150000)+1),
				ProductIDs: productIDs,
				Total:      float64(rand.Intn(5000)) + rand.Float64(),
				Status:     orderStatuses[rand.Intn(len(orderStatuses))],
				OrderDate:  time.Now().Add(-time.Duration(rand.Intn(90*24)) * time.Hour),
				CreatedAt:  time.Now().Add(-time.Duration(rand.Intn(90*24)) * time.Hour),
				UpdatedAt:  time.Now(),
			}
			orders = append(orders, order)
		}

		_, err := coll.InsertMany(context.TODO(), orders)
		if err != nil {
			log.Printf("Error inserting order batch %d: %v", batch, err)
			continue
		}

		if (batch+1)%2 == 0 {
			fmt.Printf("Inserted %d order batches (%d orders)\n", batch+1, (batch+1)*batchSize)
		}
	}
}

func printCollectionCounts(db *mongo.Database) {
	fmt.Println("\nCollection counts:")
	
	productCount, _ := db.Collection("products").CountDocuments(context.TODO(), bson.D{})
	fmt.Printf("Products: %d\n", productCount)
	
	userCount, _ := db.Collection("users").CountDocuments(context.TODO(), bson.D{})
	fmt.Printf("Users: %d\n", userCount)
	
	orderCount, _ := db.Collection("orders").CountDocuments(context.TODO(), bson.D{})
	fmt.Printf("Orders: %d\n", orderCount)
	
	total := productCount + userCount + orderCount
	fmt.Printf("Total: %d\n", total)
}