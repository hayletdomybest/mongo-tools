package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	loadEnv()

	mongoURI := os.Getenv("MONGO_URI")
	dbName := os.Getenv("MONGO_DB")
	jsonPath := os.Getenv("JSON_PATH")

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Mongo connect error: %v", err)
	}
	defer client.Disconnect(context.TODO())

	db := client.Database(dbName)

	fi, err := os.Stat(jsonPath)
	if err != nil {
		log.Fatalf("Invalid JSON_PATH: %v", err)
	}

	if fi.IsDir() {
		files, err := filepath.Glob(filepath.Join(jsonPath, "*.json"))
		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}

		for _, file := range files {
			processFile(db, file)
		}
	} else {
		processFile(db, jsonPath)
	}

	fmt.Println("✅ All imports completed.")
}

func loadEnv() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}
}

func processFile(db *mongo.Database, filePath string) {
	collectionName := extractCollectionName(filePath)
	if collectionName == "" {
		log.Printf("⚠️  Skipping unrecognized file: %s\n", filePath)
		return
	}

	fmt.Printf("📥 Importing %s → collection: %s\n", filepath.Base(filePath), collectionName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("❌ Failed to read file: %s (%v)\n", filePath, err)
		return
	}

	var raw interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("❌ JSON parse error: %s\n", filePath)
		return
	}

	var docs []interface{}
	switch v := raw.(type) {
	case []interface{}:
		docs = v
	case map[string]interface{}:
		docs = append(docs, v)
	default:
		log.Printf("❌ Unknown JSON structure in file: %s\n", filePath)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 清空舊資料
	if _, err := db.Collection(collectionName).DeleteMany(ctx, bson.M{}); err != nil {
		log.Printf("❌ Failed to clear collection %s: %v\n", collectionName, err)
		return
	}

	// 插入新資料
	if _, err := db.Collection(collectionName).InsertMany(ctx, docs); err != nil {
		log.Printf("❌ Failed to insert into %s: %v\n", collectionName, err)
	} else {
		fmt.Printf("✅ Inserted %d docs into %s\n", len(docs), collectionName)
	}
}

func extractCollectionName(filePath string) string {
	filename := filepath.Base(filePath)
	if !strings.HasSuffix(filename, ".json") {
		return ""
	}

	parts := strings.Split(filename, ".")
	if len(parts) < 2 {
		return ""
	}

	// Use last part before ".json" as collection name
	return parts[len(parts)-2]
}
