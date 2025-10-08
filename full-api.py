import json
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    http_method = event.get('httpMethod', 'GET')
    path = event.get('path', '')
    path_parameters = event.get('pathParameters', {}) or {}
    
    try:
        if http_method == 'GET' and path == '/users':
            return get_all_users()
        elif http_method == 'GET' and path_parameters.get('user_id'):
            user_id = path_parameters['user_id']
            return get_user_by_id(user_id)
        elif http_method == 'POST' and path == '/users':
            return create_user(event)
        elif http_method == 'PUT' and path_parameters.get('user_id'):
            user_id = path_parameters['user_id']
            return update_user(user_id, event)
        elif http_method == 'DELETE' and path_parameters.get('user_id'):
            user_id = path_parameters['user_id']
            return delete_user(user_id)
        else:
            return create_response(404, {'error': 'Route not found'})
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})

def get_all_users():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, email, first_name, last_name, age, created_at, updated_at 
                FROM users 
                ORDER BY created_at DESC
            """)
            users = cursor.fetchall()
            
            users_list = []
            for user in users:
                users_list.append(dict(user))
            
            return create_response(200, {'users': users_list})
    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}")
        return create_response(500, {'error': 'Failed to fetch users'})
    finally:
        conn.close()

def get_user_by_id(user_id):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, email, first_name, last_name, age, created_at, updated_at 
                FROM users 
                WHERE id = %s
            """, (user_id,))
            
            user = cursor.fetchone()
            
            if user:
                return create_response(200, {'user': dict(user)})
            else:
                return create_response(404, {'error': 'User not found'})
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {str(e)}")
        return create_response(500, {'error': 'Failed to fetch user'})
    finally:
        conn.close()

def create_user(event):
    try:
        body = json.loads(event.get('body', '{}'))
        
        required_fields = ['email', 'first_name', 'last_name']
        for field in required_fields:
            if not body.get(field):
                return create_response(400, {'error': f'Missing required field: {field}'})
        
        email = body['email']
        first_name = body['first_name']
        last_name = body['last_name']
        age = body.get('age')
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO users (email, first_name, last_name, age)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, email, first_name, last_name, age, created_at, updated_at
                """, (email, first_name, last_name, age))
                
                new_user = cursor.fetchone()
                conn.commit()
                
                cursor.execute("""
                    INSERT INTO user_audit (user_id, action)
                    VALUES (%s, 'CREATE')
                """, (new_user['id'],))
                conn.commit()
                
                return create_response(201, {'user': dict(new_user), 'message': 'User created successfully'})
                
        except psycopg2.IntegrityError:
            return create_response(409, {'error': 'User with this email already exists'})
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating user: {str(e)}")
            return create_response(500, {'error': 'Failed to create user'})
        finally:
            conn.close()
            
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})

def update_user(user_id, event):
    try:
        body = json.loads(event.get('body', '{}'))
        
        updatable_fields = ['email', 'first_name', 'last_name', 'age']
        update_data = {}
        
        for field in updatable_fields:
            if field in body:
                update_data[field] = body[field]
        
        if not update_data:
            return create_response(400, {'error': 'No fields to update'})
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                set_clause = ", ".join([f"{field} = %s" for field in update_data.keys()])
                values = list(update_data.values())
                values.append(user_id)
                
                cursor.execute(f"""
                    UPDATE users 
                    SET {set_clause}, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id, email, first_name, last_name, age, created_at, updated_at
                """, values)
                
                updated_user = cursor.fetchone()
                
                if updated_user:
                    cursor.execute("""
                        INSERT INTO user_audit (user_id, action)
                        VALUES (%s, 'UPDATE')
                    """, (user_id,))
                    conn.commit()
                    
                    return create_response(200, {'user': dict(updated_user), 'message': 'User updated successfully'})
                else:
                    conn.rollback()
                    return create_response(404, {'error': 'User not found'})
                    
        except psycopg2.IntegrityError:
            conn.rollback()
            return create_response(409, {'error': 'User with this email already exists'})
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating user {user_id}: {str(e)}")
            return create_response(500, {'error': 'Failed to update user'})
        finally:
            conn.close()
            
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})

def delete_user(user_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM users WHERE id = %s", (user_id,))
            if not cursor.fetchone():
                return create_response(404, {'error': 'User not found'})
            
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            
            cursor.execute("""
                INSERT INTO user_audit (user_id, action)
                VALUES (%s, 'DELETE')
            """, (user_id,))
            
            conn.commit()
            
            return create_response(200, {'message': 'User deleted successfully'})
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting user {user_id}: {str(e)}")
        return create_response(500, {'error': 'Failed to delete user'})
    finally:
        conn.close()

def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, X-Amz-Date, Authorization, X-Api-Key, X-Amz-Security-Token'
        },
        'body': json.dumps(body)
    }
