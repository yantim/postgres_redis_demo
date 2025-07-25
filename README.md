# Redis + PostgreSQL Cache Example

This project demonstrates using Redis as a caching layer for PostgreSQL queries. It includes a simple `UserService` class that interacts with a PostgreSQL database and caches query results in Redis to improve performance.

## Features

- **PostgreSQL Integration**: Connects to a PostgreSQL database to perform CRUD operations on a `users` table.
- **Redis Caching**: Caches query results in Redis to reduce database load and improve response times.
- **Cache Invalidation**: Automatically invalidates cache entries when data is updated.
- **Sample Data**: Automatically populates the database with sample user data if the `users` table is empty.

## Requirements

- Python 3.x
- PostgreSQL
- Redis

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yantim/postgres_redis_demo.git
   cd postgres_redis_demo
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure PostgreSQL**:
   - Ensure PostgreSQL is running and accessible.
   - Update the connection string in `main.py` if necessary:
     ```python
     pg_conn_string = "postgresql://scraper_user:supersecretpassword@localhost:5433/scraper_db"
     ```

4. **Configure Redis**:
   - Ensure Redis is running and accessible on the default port (6379).

## Usage

Run the demo script to see the caching in action:

```bash
python main.py
```

The demo will:
- Perform a user lookup and demonstrate cache hits and misses.
- Query users by age range and cache the results.
- Update a user and show cache invalidation.
- Display Redis cache statistics.

## Code Overview

- **`UserService`**: A class that handles database operations and caching.
  - `get_user_by_id(user_id)`: Fetches a user by ID, using Redis for caching.
  - `get_users_by_age_range(min_age, max_age)`: Fetches users within an age range, using Redis for caching.
  - `create_user(name, email, age)`: Creates a new user and invalidates related caches.
  - `update_user(user_id, **kwargs)`: Updates a user's information and invalidates caches.
  - `get_cache_stats()`: Retrieves Redis cache statistics.
  - `clear_all_cache()`: Clears all cached data.
  - `close()`: Closes database and Redis connections.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For questions or support, please open an issue on the GitHub repository.
