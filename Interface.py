import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='1', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    cur = con.cursor()
    
    # Xóa bảng nếu đã tồn tại và tạo lại bảng
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename + " CASCADE;")
    cur.execute("""CREATE TABLE """ + ratingstablename + """(
        userid INTEGER, 
        movieid INTEGER, 
        rating FLOAT, 
        timestamp BIGINT
    );""")
    
    # Đọc và xử lý dữ liệu từ file
    import tempfile
    import os
    
    try:
        # Tạo file tạm thời
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.dat') as temp_file:
            with open(ratingsfilepath, 'r', encoding='utf-8') as infile:
                for line in infile:
                    # Thay :: bằng tab để dễ xử lý
                    temp_line = line.replace('::', '\t')
                    temp_file.write(temp_line)
            temp_filepath = temp_file.name
        
        # Dùng COPY để tải dữ liệu vào bảng
        with open(temp_filepath, 'r', encoding='utf-8') as file:
            cur.copy_from(file, ratingstablename, sep='\t', 
                         columns=('userid', 'movieid', 'rating', 'timestamp'))
        
        # Xóa cột timestamp theo yêu cầu
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN timestamp;")
        
    except Exception as e:
        con.rollback()
        raise e
    finally:
        if 'temp_filepath' in locals():
            os.unlink(temp_filepath)
    
    cur.close()
    con.commit()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    '''
    Function to create range partitions of the ratings table.
    Each partition table is named 'range_part{i}' with i = 0 to numberofpartitions-1.
    Ratings are split into equal ranges in [0,5].
    '''

    RANGE_TABLE_PREFIX = 'range_part'

    try:
        cur = openconnection.cursor()

        # Drop existing range_part tables if any
        for i in range(numberofpartitions):
            table_name = RANGE_TABLE_PREFIX + str(i)
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Calculate size of each range partition
        interval = 5.0 / numberofpartitions

        for i in range(numberofpartitions):
            table_name = RANGE_TABLE_PREFIX + str(i)
            # Create partition table with the same schema as original (userid, movieid, rating)
            create_query = f'''
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            '''
            cur.execute(create_query)

            # Insert rows into the correct range partition
            lower_bound = i * interval
            upper_bound = (i + 1) * interval

            if i == 0:
                # Include lower bound in first partition
                insert_query = f'''
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating >= {lower_bound} AND rating <= {upper_bound};
                '''
            else:
                insert_query = f'''
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating > {lower_bound} AND rating <= {upper_bound};
                '''
            cur.execute(insert_query)

        openconnection.commit()  # Commit all changes

    except Exception as e:
        openconnection.rollback()
        print(f'Error in rangepartition: {e}')
        raise e
    finally:
        cur.close()




def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    try:
        # Xóa các bảng phân mảnh cũ nếu tồn tại
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        # Tạo bảng và chèn dữ liệu
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                        f"SELECT userid, movieid, rating FROM "
                        f"(SELECT userid, movieid, rating, ROW_NUMBER() OVER() AS rnum FROM {ratingstablename}) AS temp "
                        f"WHERE MOD(temp.rnum-1, {numberofpartitions}) = {i};")
        
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    try:
        # Kiểm tra số lượng phân mảnh
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
        if numberofpartitions == 0:
            raise Exception("No round-robin partitions exist. Please run roundrobinpartition first.")
        
        # Đếm số dòng hiện tại trong bảng chính trước khi chèn
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]
        
        # Tính phân mảnh đích dựa trên số dòng hiện tại + 1
        index = (total_rows + 1) % numberofpartitions
        table_name = RROBIN_TABLE_PREFIX + str(index)
        
        # Chèn vào bảng chính
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) "
                    f"VALUES ({userid}, {itemid}, {rating});")
        
        # Chèn vào phân mảnh
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                    f"VALUES ({userid}, {itemid}, {rating});")
        
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    conn = openconnection
    cur = conn.cursor()
    
    try:
        # Chèn vào bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        # Lấy số phân mảnh
        num_partitions = count_partitions('range_part', openconnection)
        if num_partitions == 0:
            raise Exception("No range partitions exist. Please run rangepartition first.")
        
        # Tính ranh giới phân mảnh
        delta = 5.0 / num_partitions
        for i in range(num_partitions):
            min_range = i * delta
            max_range = (i + 1) * delta
            # Kiểm tra rating thuộc phân mảnh nào
            if (i == 0 and 0 <= rating <= max_range) or \
               (i > 0 and min_range < rating <= max_range):
                cur.execute(f"""
                    INSERT INTO range_part{i} (userid, movieid, rating)
                    VALUES (%s, %s, %s);
                """, (userid, itemid, rating))
                break
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Kiểm tra xem CSDL đã tồn tại chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print('Cơ sở dữ liệu có tên {0} đã tồn tại'.format(dbname))

    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    return count