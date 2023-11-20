import csv

from mysql.connector import connect, errors
from sklearn.metrics.pairwise import cosine_similarity

# Connect to the database server
connection = connect(
    host='astronaut.snu.ac.kr',
    port=7000,
    user='DB2016_10399',
    password='DB2016_10399',
    db='DB2016_10399'
)
cursor = connection.cursor(dictionary=True)


def print_table(columns, rows):
    # Set the width of each column (based on the longest value for each column)
    width_dict = {column: len(column) + 2 for column in columns}
    for row in rows:
        for column in columns:
            width_dict[column] = max(width_dict[column], len(str(row[column])) + 2)

    # Print all rows
    dividing_line = '-' * sum(width_dict.values())
    print(dividing_line)
    print(''.join([f'{f"{column}":<{width_dict[column]}}' for column in columns]))
    print(dividing_line)
    if rows:
        print('\n'.join([
            ''.join([f'{f"{str(row[column])}":<{width_dict[column]}}' for column in columns])
            for row in rows
        ]))
        print(dividing_line)


def check_existence(table, object_id):
    # Error: Invalid ID (treat as non-existing error)
    if not object_id.isdigit():
        return False

    # Fetch the object
    cursor.execute(f'SELECT id FROM {table} WHERE id = %s;', [int(object_id)])

    # Error: Non-existing object
    if not cursor.fetchall():
        return False

    return True


def check_initialization():
    # Fetch all tables
    cursor.execute('SHOW TABLES;')
    tables = cursor.fetchall()

    # True if the tables exist, False otherwise
    return bool(tables)


def initialization_required(func):
    def wrapper(*args, **kwargs):
        # Error: Not initialized
        if not check_initialization():
            print('Database initialization is required')
            return

        return func(*args, **kwargs)

    return wrapper


# Problem 1 (5 pt.)
def initialize_database():
    # Error: Already initialized
    if check_initialization():
        print('Database already initialized')
        return

    # Create `movie` table (영화: ID, 제목, 감독, 가격)
    cursor.execute('''
        CREATE TABLE movie (
            id INT AUTO_INCREMENT,
            title VARCHAR(128) NOT NULL,
            director VARCHAR(64) NOT NULL,
            price INT NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (title)
        );
    ''')

    # Create `user` table (고객: ID, 이름, 나이)
    cursor.execute('''
        CREATE TABLE user (
            id INT AUTO_INCREMENT,
            name VARCHAR(32) NOT NULL,
            age INT NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (name, age)
        );
    ''')

    # Create `reservation` table (예매: 영화 ID, 고객 ID, 평점)
    cursor.execute('''
        CREATE TABLE reservation (
            movie_id INT NOT NULL,
            user_id INT NOT NULL,
            rating INT,
            FOREIGN KEY (movie_id) REFERENCES movie(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE,
            UNIQUE (movie_id, user_id)
        );
    ''')

    # The mapping tables for finding ID of each movie or user
    movie_to_id = {}
    user_to_id = {}

    # The maaping tables for finding reservations of each movie
    movie_to_users = {}

    # Read `data.csv` file for initialization
    with open('data.csv', 'r') as file:
        rows = list(csv.reader(file))[1:]  # Skip the header

        # For each row
        for row in rows:
            # Extract the attribute values
            title, director, price, name, age = row

            # Error: Duplicated reservation
            if title in movie_to_users and (name, age) in movie_to_users[title]:
                print(f'User {user_to_id[(name, age)]} already booked movie {movie_to_id[title]}')
                continue

            # Error: Too many reservations
            if title in movie_to_users and len(movie_to_users[title]) >= 10:
                print(f'Movie {movie_to_id[title]} has already been fully booked')
                continue

            # Insert a new movie
            if title not in movie_to_id:
                # Error: Invalid price
                if not (price.isdigit() and 0 <= int(price) <= 100000):
                    print('Movie price should be from 0 to 100000')
                    continue

                cursor.execute(
                    'INSERT INTO movie (title, director, price) VALUES (%s, %s, %s);',
                    [title, director, int(price)]
                )
                movie_to_id[title] = cursor.lastrowid

            # Insert a new user
            if (name, age) not in user_to_id:
                # Error: Invalid age
                if not (age.isdigit() and 12 <= int(age) <= 110):
                    print('User age should be from 12 to 110')
                    continue

                cursor.execute(
                    'INSERT INTO user (name, age) VALUES (%s, %s);',
                    [name, int(age)]
                )
                user_to_id[(name, age)] = cursor.lastrowid

            # Insert a new reservation
            cursor.execute(
                'INSERT INTO reservation (movie_id, user_id) VALUES (%s, %s);',
                [movie_to_id[title], user_to_id[(name, age)]]
            )
            if title not in movie_to_users:
                movie_to_users[title] = []
            movie_to_users[title].append((name, age))

    # Commit the changes to the database
    connection.commit()

    print('Database successfully initialized')


# Problem 2 (4 pt.)
@initialization_required
def print_movies():
    # Fetch all movies
    cursor.execute('''
        SELECT id, title, director, price, COUNT(movie_id) AS reservation, AVG(rating) AS avg_rating 
        FROM movie
        LEFT OUTER JOIN reservation ON (id = movie_id)
        GROUP BY id
        ORDER BY id;
    ''')
    movies = cursor.fetchall()

    # Print all movies
    print_table(['id', 'title', 'director', 'price', 'reservation', 'avg_rating'], movies)


# Problem 3 (4 pt.)
@initialization_required
def print_users():
    # Fetch all users
    cursor.execute('SELECT id, name, age FROM user ORDER BY id;')
    users = cursor.fetchall()

    # Print all users
    print_table(['id', 'name', 'age'], users)


# Problem 4 (4 pt.)
@initialization_required
def insert_movie():
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')
    print()

    # Error: Invalid price
    if not (price.isdigit() and 0 <= int(price) <= 100000):
        print('Movie price should be from 0 to 100000')
        return

    # Insert a new movie
    try:
        cursor.execute(
            'INSERT INTO movie (title, director, price) VALUES (%s, %s, %s);',
            [title, director, int(price)]
        )

    # Error: Duplicated movie
    except errors.IntegrityError:
        print(f'Movie {title} already exists')
        return

    # Commit the changes to the database
    connection.commit()

    print('One movie successfully inserted')


# Problem 6 (4 pt.)
@initialization_required
def remove_movie():
    movie_id = input('Movie ID: ')
    print()

    # Error: Non-existing movie
    if not check_existence('movie', movie_id):
        print(f'Movie {movie_id} does not exist')
        return

    # Remove the movie
    cursor.execute('DELETE FROM movie WHERE id = %s;', [int(movie_id)])

    # Commit the changes to the database
    connection.commit()

    print('One movie successfully removed')


# Problem 5 (4 pt.)
@initialization_required
def insert_user():
    name = input('User name: ')
    age = input('User age: ')
    print()

    # Error: Invalid age
    if not (age.isdigit() and 12 <= int(age) <= 110):
        print('User age should be from 12 to 110')
        return

    # Insert a new user
    try:
        cursor.execute(
            'INSERT INTO user (name, age) VALUES (%s, %s);',
            [name, int(age)]
        )

    # Error: Duplicated user
    except errors.IntegrityError:
        print(f'User ({name}, {age}) already exists')
        return

    # Commit the changes to the database
    connection.commit()

    print('One user successfully inserted')


# Problem 7 (4 pt.)
@initialization_required
def remove_user():
    user_id = input('User ID: ')
    print()

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # Remove the user
    cursor.execute('DELETE FROM user WHERE id = %s;', [int(user_id)])

    # Commit the changes to the database
    connection.commit()

    print('One user successfully removed')


# Problem 8 (5 pt.)
@initialization_required
def book_movie():
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    print()

    # Error: Non-existing movie
    if not check_existence('movie', movie_id):
        print(f'Movie {movie_id} does not exist')
        return

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # Fetch all users who booked for the movie
    cursor.execute('SELECT user_id FROM reservation WHERE movie_id = %s;', [int(movie_id)])
    user_ids = [user['user_id'] for user in cursor.fetchall()]

    # Error: Duplicated reservation
    if int(user_id) in user_ids:
        print(f'User {user_id} already booked movie {movie_id}')
        return

    # Error: Too many reservations
    if len(user_ids) >= 10:
        print(f'Movie {movie_id} has already been fully booked')
        return

    # Insert a new reservation
    cursor.execute(
        'INSERT INTO reservation (movie_id, user_id) VALUES (%s, %s);',
        [int(movie_id), int(user_id)]
    )

    # Commit the changes to the database
    connection.commit()

    print('Movie successfully booked')


# Problem 9 (5 pt.)
@initialization_required
def rate_movie():
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')
    print()

    # Error: Non-existing movie
    if not check_existence('movie', movie_id):
        print(f'Movie {movie_id} does not exist')
        return

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # Error: Invalid rating
    if not (rating.isdigit() and 1 <= int(rating) <= 5):
        print('Wrong value for a rating')
        return

    # Fetch the reservation
    cursor.execute(
        'SELECT rating FROM reservation WHERE movie_id = %s AND user_id = %s;',
        [int(movie_id), int(user_id)]
    )
    reservations = cursor.fetchall()

    # Error: Not booked yet
    if not reservations:
        print(f'User {user_id} has not booked movie {movie_id} yet')
        return

    # Error: Already rated
    if reservations[0]['rating']:
        print(f'User {user_id} has already rated movie {movie_id}')
        return

    # Update the reservation (reflect the rating)
    cursor.execute(
        'UPDATE reservation SET rating = %s WHERE movie_id = %s AND user_id = %s;',
        [int(rating), int(movie_id), int(user_id)]
    )

    # Commit the changes to the database
    connection.commit()

    print('Movie successfully rated')


# Problem 10 (5 pt.)
@initialization_required
def print_users_for_movie():
    movie_id = input('Movie ID: ')
    print()

    # Error: Non-existing movie
    if not check_existence('movie', movie_id):
        print(f'Movie {movie_id} does not exist')
        return

    # Fetch all users(including rating) who booked for the movie
    cursor.execute(
        '''
            SELECT id, name, age, rating
            FROM user
            INNER JOIN reservation ON (id = user_id)
            WHERE movie_id = %s
            ORDER BY id;
        ''',
        [int(movie_id)]
    )
    users = cursor.fetchall()

    # Print all users
    print_table(['id', 'name', 'age', 'rating'], users)


# Problem 11 (5 pt.)
@initialization_required
def print_movies_for_user():
    user_id = input('User ID: ')
    print()

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # Fetch all movies(including rating) booked by the user
    cursor.execute(
        '''
            SELECT id, title, director, price, rating
            FROM movie
            INNER JOIN reservation ON (id = movie_id)
            WHERE user_id = %s
            ORDER BY id;
        ''',
        [int(user_id)]
    )
    movies = cursor.fetchall()

    # Print all movies
    print_table(['id', 'title', 'director', 'price', 'rating'], movies)


# Problem 12 (6 pt.)
@initialization_required
def recommend_popularity():
    user_id = input('User ID: ')

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # The subquery for fetching unseen movies
    unseen_movies = '''
        SELECT id, title, director, price, COUNT(movie_id) AS reservation, AVG(rating) AS avg_rating
        FROM movie
        LEFT OUTER JOIN reservation ON (id = movie_id)
        WHERE id NOT IN (SELECT movie_id FROM reservation WHERE user_id = %s)
        GROUP BY id
    '''

    def recommend_by_rating():
        print('\nRating-based\n')

        # Fetch the movie to recommend, based on ratings
        cursor.execute(f'SELECT * FROM ({unseen_movies}) AS m ORDER BY avg_rating DESC, id LIMIT 1', [int(user_id)])
        movies = cursor.fetchall()

        print_table(['id', 'title', 'director', 'price', 'reservation', 'avg_rating'], movies)

    def recommend_by_popularity():
        print('\nPopularity-based\n')

        # Fetch the movie to recommend, based on popularity
        cursor.execute(f'SELECT * FROM ({unseen_movies}) AS m ORDER BY reservation DESC, id LIMIT 1', [int(user_id)])
        movies = cursor.fetchall()

        print_table(['id', 'title', 'director', 'price', 'reservation', 'avg_rating'], movies)

    recommend_by_rating()
    recommend_by_popularity()


# Problem 13 (10 pt.)
@initialization_required
def recommend_item_based():
    user_id = input('User ID: ')
    print()

    # Error: Non-existing user
    if not check_existence('user', user_id):
        print(f'User {user_id} does not exist')
        return

    # Fetch unrated movies
    cursor.execute(
        '''
            SELECT id
            FROM movie
            WHERE id NOT IN (SELECT movie_id FROM reservation WHERE user_id = %s AND rating IS NOT NULL)
            ORDER BY id;
        ''',
        [int(user_id)]
    )
    unrated_movie_ids = [movie['id'] for movie in cursor.fetchall()]

    # No movie to recommend, since the user rated all movies
    if not unrated_movie_ids:
        print_table(['id', 'title', 'director', 'price', 'avg_rating', 'expected_rating'], [])

    # Fetch all users
    cursor.execute('SELECT id FROM user ORDER BY id;')
    user_ids = [user['id'] for user in cursor.fetchall()]
    user_id_to_idx = {uid: idx for idx, uid in enumerate(user_ids)}  # Map ID to idx

    # Fetch all movies
    cursor.execute('SELECT id FROM movie ORDER BY id;')
    movie_ids = [movie['id'] for movie in cursor.fetchall()]
    movie_id_to_idx = {mid: idx for idx, mid in enumerate(movie_ids)}  # Map ID to idx

    # Fetch all reservations with rating value
    cursor.execute('SELECT user_id, movie_id, rating FROM reservation WHERE rating IS NOT NULL;')
    reservations = cursor.fetchall()

    def calculate_rating_matrix():
        # Initialize the rating matrix, where each cell contains a rating value
        rating_matrix = [[None] * len(movie_ids) for _ in user_ids]

        # Fill the rating matrix
        for reservation in reservations:
            i = user_id_to_idx[reservation['user_id']]
            j = movie_id_to_idx[reservation['movie_id']]
            rating_matrix[i][j] = reservation['rating']

        # For each row, replace `None` with the average of the rating values in the same row
        for i, row in enumerate(rating_matrix):
            ratings = [rating for rating in row if rating is not None]

            if not ratings:
                avg_rating = 0
            else:
                avg_rating = sum(ratings) / len(ratings)

            for j, rating in enumerate(row):
                if rating is None:
                    rating_matrix[i][j] = avg_rating

        # TODO (Round each rating value to 2 decimal places)
        # rating_matrix = [[round(rating, 2) for rating in row] for row in rating_matrix]

        return rating_matrix

    def calculate_similarity_matrix(rating_matrix):
        # Calculate the similarity matrix, based on cosine similarity
        similarity_matrix = cosine_similarity(rating_matrix)

        # TODO (Round each similarity value to 2 decimal places)
        # similarity_matrix = [[round(similarity, 2) for similarity in row] for row in similarity_matrix]

        return similarity_matrix

    # Calculate the rating matrix
    rating_matrix = calculate_rating_matrix()

    # Error: No ratings exist for the user who wants recommendation
    if all(map(lambda x: x == 0, rating_matrix[user_id_to_idx[int(user_id)]])):
        print('Rating does not exist')
        return

    # Calculate the similarity matrix
    similarity_matrix = calculate_similarity_matrix(rating_matrix)

    # The row of the similarity_matrix, corresponding to the user who wants recommendation
    similarity_matrix_row = similarity_matrix[user_id_to_idx[int(user_id)]]

    # The information of the movie to recommend
    recommended_movie_idx = None
    recommended_movie_rating = float('-inf')

    # For each unrated movie, predict the rating value
    for unrated_movie_id in unrated_movie_ids:
        total_weights = 0
        expected_rating = 0

        j = movie_id_to_idx[unrated_movie_id]
        for i in range(len(user_ids)):
            if i != user_id_to_idx[int(user_id)]:
                weight = similarity_matrix_row[i]
                rating = rating_matrix[i][j]

                total_weights += weight
                expected_rating += weight * rating

        expected_rating /= total_weights

        # Update the movie to recommend
        if expected_rating > recommended_movie_rating:
            recommended_movie_idx = j
            recommended_movie_rating = expected_rating

    # TODO (Round the expected rating value to 2 decimal places)
    # recommended_movie_rating = round(recommended_movie_rating, 2)

    # Fetch the movie to recommend
    cursor.execute(
        '''
            SELECT id, title, director, price, AVG(rating) AS avg_rating
            FROM movie
            LEFT OUTER JOIN reservation ON (id = movie_id)
            WHERE id = %s
            GROUP BY id
        ''',
        [movie_ids[recommended_movie_idx]]
    )
    recommended_movies = cursor.fetchall()

    # Append `expected_rating` pseudo-attribute manually (for printing)
    recommended_movie = {
        **recommended_movies[0],
        'expected_rating': recommended_movie_rating
    }

    print_table(['id', 'title', 'director', 'price', 'avg_rating', 'expected_rating'], [recommended_movie])


# Problem 15 (5 pt.)
def reset():
    # Prompt for confirming reset
    yn = input('Reset your database (y/n): ').lower()
    if yn != 'y':
        return

    # Disable foreign key checks on DROP TABLE query
    cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')

    # Fetch all tables
    cursor.execute('SHOW TABLES;')
    tables = cursor.fetchall()

    # Drop all tables
    for table in tables:
        drop_table_query = f'DROP TABLE {list(table.values())[0]};'
        cursor.execute(drop_table_query)

    # Enable foreign key checks on DROP TABLE query
    cursor.execute('SET FOREIGN_KEY_CHECKS = 1;')

    # Commit the changes to the database
    connection.commit()

    print()

    # Initialized the database
    initialize_database()


# Total of 70 pt.
def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies booked by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using user-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================\n')

        menu = int(input('Select your action: '))

        if menu == 14:
            print('Bye!')
            break

        print()
        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 15:
            reset()
        else:
            print('Invalid action')
        print()


if __name__ == '__main__':
    main()
