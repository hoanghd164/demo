import mysql.connector
from mysql.connector import Error
import os
import sys
import unidecode 

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def db_querry(query=None):
    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_passwd,
            database=db_name,
            connect_timeout=2
        )
        if query is not None:
            cursor = connection.cursor()
            cursor.execute(query)
            db_results = cursor.fetchall()
            return db_results

        if connection.is_connected():
            if query is not None:
                cursor.close()
            connection.close()

    except Error as e:
        raise Exception(f"Error: {str(e)}")

def rocket_cache():
    list_rocket_cache = []
    
    rocket_cache_completed = {
        "name": "rocket_cache_completed",
        "role": "rocket_cache",
        "type": "completed",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": db_querry("SELECT COUNT(*) AS completed_count FROM wp_wpr_rocket_cache WHERE status = 'completed';")[0][0]
    }
    
    rocket_cache_total = {
        "name": "rocket_cache_total",
        "role": "rocket_cache",
        "type": "total",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": db_querry("SELECT COUNT(*) AS total_count FROM wp_wpr_rocket_cache;")[0][0]
    }
    
    rocket_cache_incomplete = {
        "name": "rocket_cache_incomplete",
        "role": "rocket_cache",
        "type": "incomplete",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": db_querry("SELECT COUNT(*) AS incomplete_count FROM wp_wpr_rocket_cache WHERE status = 'incomplete';")[0][0]
    }
    
    rocket_cache_failed = {
        "name": "rocket_cache_failed",
        "role": "rocket_cache",
        "type": "failed",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": db_querry("SELECT COUNT(*) AS failed_count FROM wp_wpr_rocket_cache WHERE status = 'failed';")[0][0]
    }
    
    list_rocket_cache.append(rocket_cache_completed)
    list_rocket_cache.append(rocket_cache_total)
    list_rocket_cache.append(rocket_cache_incomplete)
    list_rocket_cache.append(rocket_cache_failed)
    
    return list_rocket_cache

def find_post_ids_with_keywords(keywords):
    list_id_match_keywords = []
    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_passwd,
            database=db_name,
            connect_timeout=2
        )
        with connection.cursor() as cursor:
            matching_posts = []
            for keyword in keywords:
                sql_select = "SELECT ID, post_status, post_name, post_content FROM wp_posts WHERE post_content LIKE %s AND post_type = 'post'"
                cursor.execute(sql_select, ('%' + keyword + '%',))
                results = cursor.fetchall()
                for result in results:
                    post_id = result[0]
                    post_status = result[1]
                    post_name = result[2]
                    post_content = result[3]

                    matched_keywords = [kw for kw in keywords if kw in post_content]
                    matched_keywords = [unidecode.unidecode(kw).replace(' ', '_') for kw in matched_keywords]
                    if matched_keywords:  # Only add posts with matched keywords
                        list_id_match_keywords.append(post_id)
                        post = {
                            'id': post_id,
                            'post_status': post_status,
                            'post_name': post_name,
                            'matched_keywords': '; '.join(matched_keywords)
                        }
                        if post not in matching_posts:
                            matching_posts.append(post)
            return matching_posts, list_id_match_keywords
    finally:
        connection.close()

def wordpress_stats():
    list_wp_stats = []

    total_posts = {
        "name": "total_posts",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT COUNT(*) AS total_posts FROM wp_posts WHERE post_status = 'publish';")[0][0])
    }

    total_comments = {
        "name": "total_comments",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT COUNT(*) AS total_comments FROM wp_comments WHERE comment_approved = 1;")[0][0])
    }

    total_users = {
        "name": "total_users",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT COUNT(*) AS total_users FROM wp_users;")[0][0])
    }

    total_pages = {
        "name": "total_pages",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT COUNT(*) AS total_pages FROM wp_posts WHERE post_type = 'page' AND post_status = 'publish';")[0][0])
    }

    total_categories = {
        "name": "total_categories",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT COUNT(*) AS total_categories FROM wp_terms INNER JOIN wp_term_taxonomy ON wp_terms.term_id = wp_term_taxonomy.term_id WHERE wp_term_taxonomy.taxonomy = 'category';")[0][0])
    }

    posts_per_category = db_querry("SELECT wp_terms.slug AS category_name, COUNT(wp_posts.ID) AS post_count FROM wp_terms INNER JOIN wp_term_taxonomy ON wp_terms.term_id = wp_term_taxonomy.term_id INNER JOIN wp_term_relationships ON wp_term_taxonomy.term_taxonomy_id = wp_term_relationships.term_taxonomy_id INNER JOIN wp_posts ON wp_term_relationships.object_id = wp_posts.ID WHERE wp_term_taxonomy.taxonomy = 'category' AND wp_posts.post_status = 'publish' GROUP BY wp_terms.name;")
    for category in posts_per_category:
        list_wp_stats.append({
            "name": "posts_per_category",
            "role": "wordpress_stats",
            "domain": domain,
            "db_host": db_host,
            "db_port": str(db_port),
            "category_name": category[0],
            "value": float(category[1])
        })

    total_views = {
        "name": "total_views",
        "role": "wordpress_stats",
        "domain": domain,
        "db_host": db_host,
        "db_port": str(db_port),
        "value": float(db_querry("SELECT SUM(meta_value) AS total_views FROM wp_postmeta WHERE meta_key = 'post_views_count';")[0][0])
    }

    views_per_post = db_querry("SELECT wp_posts.ID, wp_posts.post_title, wp_postmeta.meta_value AS views, wp_posts.post_name FROM wp_posts INNER JOIN wp_postmeta ON wp_posts.ID = wp_postmeta.post_id WHERE wp_postmeta.meta_key = 'post_views_count' AND wp_posts.post_status = 'publish';")

    for post in views_per_post:
        list_wp_stats.append({
            "name": "views_per_post",
            "role": "wordpress_stats",
            "domain": domain,
            "db_host": db_host,
            "db_port": str(db_port),
            "post_id": str(post[0]),
            "post_name": post[3],
            "value": float(post[2])
        })

    views_per_category = db_querry("SELECT wp_terms.slug AS category_name, COUNT(wp_posts.ID) AS post_count FROM wp_terms INNER JOIN wp_term_taxonomy ON wp_terms.term_id = wp_term_taxonomy.term_id INNER JOIN wp_term_relationships ON wp_term_taxonomy.term_taxonomy_id = wp_term_relationships.term_taxonomy_id INNER JOIN wp_posts ON wp_term_relationships.object_id = wp_posts.ID WHERE wp_term_taxonomy.taxonomy = 'category' AND wp_posts.post_status = 'publish' GROUP BY wp_terms.name;")

    for category in views_per_category:
        list_wp_stats.append({
            "name": "views_per_category",
            "role": "wordpress_stats",
            "domain": domain,
            "db_host": db_host,
            "db_port": str(db_port),
            "category_name": category[0],
            "value": float(category[1])
        })

    posts_with_keywords, list_id_match_keywords = find_post_ids_with_keywords(keywords)
    for post in posts_with_keywords:
        list_wp_stats.append({
            "name": "posts_with_keywords",
            "role": "wordpress_stats",
            "domain": domain,
            "db_host": db_host,
            "db_port": str(db_port),
            "post_name": post['post_name'],
            "post_status": post['post_status'],
            "matched_keywords": post['matched_keywords'],
            "value": post['id']
        })

    list_wp_stats.append(total_posts)
    list_wp_stats.append(total_comments)
    list_wp_stats.append(total_users)
    list_wp_stats.append(total_pages)
    list_wp_stats.append(total_categories)
    list_wp_stats.append(total_views)
    
    return list_wp_stats, list_id_match_keywords

if __name__ == '__main__':
    final_results = []
    final_errors = []
    sensor_name = 'wordpress'

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", [])
        for target in targets:
            if not target.get("enable", False):
                continue

            domain = target['domain']
            db_host = target['db_host']
            db_port = target['db_port']
            db_user = target['db_user']
            db_passwd = target['db_passwd']
            db_name = target['db_name']
            wp_user = target.get('wp_user', 'admin')
            wp_passwd = target.get('wp_passwd', '')
            keywords = target.get('matched_keywords', [])

            globals().update(locals())

            try:
                final_results.extend(rocket_cache())
                stats, _ = wordpress_stats()
                final_results.extend(stats)
            except Exception as target_err:
                final_errors.append({
                    "name": "wordpress_error",
                    "role": "wordpress",
                    "domain": domain,
                    "db_host": db_host,
                    "db_port": str(db_port),
                    "message": str(target_err).replace('"', "'"),
                    "value": 1
                })

        final_results.extend(final_errors)

    except Exception as e:
        print(f"❌ {sensor_name} failed: {e}", file=sys.stderr)
        final_results = [{
            "name": "wordpress_error",
            "role": "wordpress",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)