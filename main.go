package main

import (
	"bufio"
	"bytes"
	"context"
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
	coll := extractCollectionName(filePath)
	if coll == "" {
		log.Printf("⚠️  Skipping unrecognized file: %s\n", filePath)
		return
	}

	fmt.Printf("📥 Importing %s → collection: %s\n", filepath.Base(filePath), coll)

	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("❌ Failed to read file: %s (%v)\n", filePath, err)
		return
	}

	docs, err := parseExtendedJSON(data)
	if err != nil {
		log.Printf("❌ Failed to parse Extended JSON in %s: %v\n", filePath, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 清空舊資料
	if _, err := db.Collection(coll).DeleteMany(ctx, bson.M{}); err != nil {
		log.Printf("❌ Failed to clear collection %s: %v\n", coll, err)
		return
	}

	// 插入新資料
	if _, err := db.Collection(coll).InsertMany(ctx, docs); err != nil {
		log.Printf("❌ Failed to insert into %s: %v\n", coll, err)
	} else {
		fmt.Printf("✅ Inserted %d docs into %s\n", len(docs), coll)
	}
}

// parseExtendedJSON 支援 整份 JSON Array 或 NDJSON，每笔都用 relaxed 模式解析 Extended JSON
func parseExtendedJSON(data []byte) ([]interface{}, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	var docs []interface{}

	// 整份 JSON Array
	if data[0] == '[' {
		var arr []bson.M
		// <--- relaxed 模式：false
		if err := bson.UnmarshalExtJSON(data, false, &arr); err != nil {
			return nil, fmt.Errorf("failed to parse JSON array: %v", err)
		}
		for _, m := range arr {
			docs = append(docs, m)
		}
		return docs, nil
	}

	// 否则当作 NDJSON（每行一笔）
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var m bson.M
		// <--- relaxed 模式：false
		if err := bson.UnmarshalExtJSON([]byte(line), false, &m); err != nil {
			return nil, fmt.Errorf("failed to parse line as Extended JSON: %v", err)
		}
		docs = append(docs, m)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return docs, nil
}

func extractCollectionName(filePath string) string {
	name := filepath.Base(filePath)
	if !strings.HasSuffix(name, ".json") {
		return ""
	}
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2]
}
