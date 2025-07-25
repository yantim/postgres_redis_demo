# Redis + PostgreSQL Cache Example
# This demonstrates using Redis as a cache layer for PostgreSQL queries

import psycopg2
import redis
import json
import time
from typing import Optional, List, Dict

class UserService:
    def __init__(self, pg_connection_string: str, redis_host: str = 'localhost', redis_port: int = 6379):
        # PostgreSQL connection
        self.pg_conn = psycopg2.connect(pg_connection_string)
        self.pg_cursor = self.pg_conn.cursor()
        
        # Redis connection
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Initialize database
        self._setup_database()
    
    def _setup_database(self):
        """Create users table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.pg_cursor.execute(create_table_query)
        self.pg_conn.commit()
        
        # Insert sample data if table is empty
        self.pg_cursor.execute("SELECT COUNT(*) FROM users")
        if self.pg_cursor.fetchone()[0] == 0:
            sample_users = [
                ('Alice Johnson', 'alice@example.com', 28),
                ('Bob Smith', 'bob@example.com', 34),
                ('Charlie Brown', 'charlie@example.com', 22),
                ('Diana Wilson', 'diana@example.com', 31),
                ('Eve Davis', 'eve@example.com', 26)
            ]
            
            insert_query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)"
            self.pg_cursor.executemany(insert_query, sample_users)
            self.pg_conn.commit()
            print("Sample data inserted into PostgreSQL")

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID with Redis caching"""
        cache_key = f"user:{user_id}"
        
        # Try to get from Redis cache first
        print(f"🔍 Checking Redis cache for user {user_id}")
        cached_user = self.redis_client.get(cache_key)
        
        if cached_user:
            print("✅ Cache HIT - returning from Redis")
            return json.loads(cached_user)
        
        # Cache miss - query PostgreSQL
        print("❌ Cache MISS - querying PostgreSQL")
        start_time = time.time()
        
        query = "SELECT id, name, email, age, created_at FROM users WHERE id = %s"
        self.pg_cursor.execute(query, (user_id,))
        result = self.pg_cursor.fetchone()
        
        db_query_time = time.time() - start_time
        print(f"📊 PostgreSQL query took: {db_query_time:.4f} seconds")
        
        if result:
            user_data = {
                'id': result[0],
                'name': result[1],
                'email': result[2],
                'age': result[3],
                'created_at': result[4].isoformat() if result[4] else None
            }
            
            # Store in Redis cache with 5-minute expiration
            self.redis_client.setex(cache_key, 300, json.dumps(user_data))
            print("💾 Stored in Redis cache (expires in 5 minutes)")
            
            return user_data
        
        return None

    def get_users_by_age_range(self, min_age: int, max_age: int) -> List[Dict]:
        """Get users by age range with Redis caching"""
        cache_key = f"users:age:{min_age}-{max_age}"
        
        print(f"🔍 Checking Redis cache for age range {min_age}-{max_age}")
        cached_users = self.redis_client.get(cache_key)
        
        if cached_users:
            print("✅ Cache HIT - returning from Redis")
            return json.loads(cached_users)
        
        print("❌ Cache MISS - querying PostgreSQL")
        start_time = time.time()
        
        query = "SELECT id, name, email, age FROM users WHERE age BETWEEN %s AND %s ORDER BY age"
        self.pg_cursor.execute(query, (min_age, max_age))
        results = self.pg_cursor.fetchall()
        
        db_query_time = time.time() - start_time
        print(f"📊 PostgreSQL query took: {db_query_time:.4f} seconds")
        
        users_data = [
            {'id': row[0], 'name': row[1], 'email': row[2], 'age': row[3]}
            for row in results
        ]
        
        # Cache for 2 minutes
        self.redis_client.setex(cache_key, 120, json.dumps(users_data))
        print("💾 Stored in Redis cache (expires in 2 minutes)")
        
        return users_data

    def create_user(self, name: str, email: str, age: int) -> Dict:
        """Create a new user and invalidate related caches"""
        query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s) RETURNING id, created_at"
        self.pg_cursor.execute(query, (name, email, age))
        result = self.pg_cursor.fetchone()
        self.pg_conn.commit()
        
        user_data = {
            'id': result[0],
            'name': name,
            'email': email,
            'age': age,
            'created_at': result[1].isoformat()
        }
        
        # Invalidate age range caches (simple approach - delete all age range keys)
        pattern = "users:age:*"
        for key in self.redis_client.scan_iter(match=pattern):
            self.redis_client.delete(key)
        print("🗑️ Invalidated age range caches")
        
        return user_data

    def update_user(self, user_id: int, **kwargs) -> Optional[Dict]:
        """Update user and invalidate cache"""
        # Build dynamic update query
        set_clause = ", ".join([f"{key} = %s" for key in kwargs.keys()])
        values = list(kwargs.values()) + [user_id]
        
        query = f"UPDATE users SET {set_clause} WHERE id = %s RETURNING id, name, email, age, created_at"
        self.pg_cursor.execute(query, values)
        result = self.pg_cursor.fetchone()
        self.pg_conn.commit()
        
        if result:
            # Invalidate specific user cache
            cache_key = f"user:{user_id}"
            self.redis_client.delete(cache_key)
            print(f"🗑️ Invalidated cache for user {user_id}")
            
            # Also invalidate age range caches if age was updated
            if 'age' in kwargs:
                pattern = "users:age:*"
                for key in self.redis_client.scan_iter(match=pattern):
                    self.redis_client.delete(key)
                print("🗑️ Invalidated age range caches")
            
            return {
                'id': result[0],
                'name': result[1],
                'email': result[2],
                'age': result[3],
                'created_at': result[4].isoformat()
            }
        
        return None

    def get_cache_stats(self) -> Dict:
        """Get Redis cache statistics"""
        info = self.redis_client.info()
        return {
            'connected_clients': info['connected_clients'],
            'used_memory_human': info['used_memory_human'],
            'keyspace_hits': info['keyspace_hits'],
            'keyspace_misses': info['keyspace_misses'],
            'hit_rate': info['keyspace_hits'] / (info['keyspace_hits'] + info['keyspace_misses']) * 100 
                      if (info['keyspace_hits'] + info['keyspace_misses']) > 0 else 0
        }

    def clear_all_cache(self):
        """Clear all cached data"""
        self.redis_client.flushdb()
        print("🗑️ All cache cleared")

    def close(self):
        """Close connections"""
        self.pg_cursor.close()
        self.pg_conn.close()
        self.redis_client.close()


def demo():
    """Demonstrate Redis caching with PostgreSQL"""
    # Connection string for your existing PostgreSQL container
    pg_conn_string = "postgresql://scraper_user:supersecretpassword@localhost:5433/scraper_db"
    
    try:
        service = UserService(pg_conn_string)
        
        print("=" * 60)
        print("🚀 Redis + PostgreSQL Cache Demo")
        print("=" * 60)
        
        # Demo 1: Single user lookup (cache miss then hit)
        print("\n1️⃣ DEMO: Single User Lookup")
        print("-" * 30)
        user1 = service.get_user_by_id(1)  # Cache miss
        print(f"User: {user1['name']}")
        
        print("\nSame query again:")
        user1_cached = service.get_user_by_id(1)  # Cache hit
        print(f"User: {user1_cached['name']}")
        
        # Demo 2: Age range query
        print("\n2️⃣ DEMO: Age Range Query")
        print("-" * 30)
        young_users = service.get_users_by_age_range(20, 30)  # Cache miss
        print(f"Found {len(young_users)} users aged 20-30")
        
        print("\nSame query again:")
        young_users_cached = service.get_users_by_age_range(20, 30)  # Cache hit
        print(f"Found {len(young_users_cached)} users aged 20-30")
        
        # Demo 3: Cache invalidation
        print("\n3️⃣ DEMO: Cache Invalidation")
        print("-" * 30)
        print("Updating user 1...")
        updated_user = service.update_user(1, age=29)
        print(f"Updated: {updated_user['name']} is now {updated_user['age']} years old")
        
        print("\nQuerying user 1 again (cache was invalidated):")
        user1_updated = service.get_user_by_id(1)  # Cache miss due to invalidation
        print(f"User: {user1_updated['name']}, Age: {user1_updated['age']}")
        
        # Demo 4: Cache statistics
        print("\n4️⃣ DEMO: Cache Statistics")
        print("-" * 30)
        stats = service.get_cache_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        service.close()
        
    except psycopg2.Error as e:
        print(f"PostgreSQL Error: {e}")
    except redis.RedisError as e:
        print(f"Redis Error: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    demo()