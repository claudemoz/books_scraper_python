import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import csv
import random
import time
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BooksScraperDB:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # URLs de base
        self.books_base_url = "https://books.toscrape.com"
        self.quotes_base_url = "https://quotes.toscrape.com"
        self.openlibrary_api = "https://openlibrary.org"
        
        # Données collectées
        self.categories = []
        self.books = []
        self.publishers = []
        self.authors = []
        self.quotes = []
        self.tags = set()
        
        # Maisons d'édition fictives
        self.fictional_publishers = [
            "Penguin Random House", "HarperCollins", "Macmillan Publishers",
            "Simon & Schuster", "Hachette Book Group", "Scholastic",
            "Wiley", "Pearson Education", "McGraw-Hill Education",
            "Oxford University Press", "Cambridge University Press",
            "Bloomsbury Publishing", "Faber & Faber", "Little, Brown and Company",
            "Vintage Books", "Bantam Books", "Doubleday", "Knopf",
            "Grove Atlantic", "Farrar, Straus and Giroux"
        ]

    def connect_db(self):
        """Connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            logger.info("Connexion à PostgreSQL établie")
        except Exception as e:
            logger.error(f"Erreur de connexion à PostgreSQL: {e}")
            raise

    def create_tables(self):
        """Création des tables avec contraintes FK"""
        cursor = self.conn.cursor()
        
        # Script de création des tables
        create_tables_sql = """
        -- Suppression des tables si elles existent (ordre inverse des FK)
        DROP TABLE IF EXISTS quote_tags CASCADE;
        DROP TABLE IF EXISTS book_authors CASCADE;
        DROP TABLE IF EXISTS quotes CASCADE;
        DROP TABLE IF EXISTS tags CASCADE;
        DROP TABLE IF EXISTS books CASCADE;
        DROP TABLE IF EXISTS authors CASCADE;
        DROP TABLE IF EXISTS publishers CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;

        -- Table des catégories
        CREATE TABLE categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Table des maisons d'édition
        CREATE TABLE publishers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Table des auteurs
        CREATE TABLE authors (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            birth_date DATE,
            death_date DATE,
            bio TEXT,
            openlibrary_key VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, birth_date)
        );

        -- Table des livres
        CREATE TABLE books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            price DECIMAL(10,2),
            availability VARCHAR(100),
            description TEXT,
            rating INTEGER CHECK (rating >= 1 AND rating <= 5),
            image_url VARCHAR(500),
            upc VARCHAR(50),
            publication_year INTEGER,
            category_id INTEGER REFERENCES categories(id),
            publisher_id INTEGER REFERENCES publishers(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Table de liaison livre-auteur (relation N:N)
        CREATE TABLE book_authors (
            id SERIAL PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            author_id INTEGER REFERENCES authors(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(book_id, author_id)
        );

        -- Table des tags
        CREATE TABLE tags (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Table des citations
        CREATE TABLE quotes (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            author_id INTEGER REFERENCES authors(id),
            book_source VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Table de liaison citation-tag (relation N:N)
        CREATE TABLE quote_tags (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER REFERENCES quotes(id) ON DELETE CASCADE,
            tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(quote_id, tag_id)
        );

        -- Index pour améliorer les performances
        CREATE INDEX idx_books_category ON books(category_id);
        CREATE INDEX idx_books_publisher ON books(publisher_id);
        CREATE INDEX idx_quotes_author ON quotes(author_id);
        CREATE INDEX idx_book_authors_book ON book_authors(book_id);
        CREATE INDEX idx_book_authors_author ON book_authors(author_id);
        """
        
        try:
            cursor.execute(create_tables_sql)
            self.conn.commit()
            logger.info("Tables créées avec succès")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur lors de la création des tables: {e}")
            raise
        finally:
            cursor.close()

    def extract_categories(self):
        """1. Extraire les catégories depuis books.toscrape.com"""
        logger.info("Extraction des catégories...")
        
        try:
            response = self.session.get(self.books_base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Trouver la section des catégories
            categories_section = soup.find('ul', class_='nav nav-list')
            if categories_section:
                category_links = categories_section.find_all('a')
                for link in category_links:
                    category_name = link.text.strip()
                    if category_name and category_name != "Books":
                        # Nettoyer le nom de la catégorie
                        clean_name = re.sub(r'\s+\d+$', '', category_name).strip()
                        if clean_name not in [cat['name'] for cat in self.categories]:
                            self.categories.append({
                                'name': clean_name,
                                'url': urljoin(self.books_base_url, link.get('href', ''))
                            })
            
            logger.info(f"Trouvé {len(self.categories)} catégories")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des catégories: {e}")

    def extract_rating_from_class(self, rating_class):
        """Convertir la classe CSS de note en nombre"""
        rating_map = {
            'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5
        }
        for word, num in rating_map.items():
            if word in rating_class:
                return num
        return None

    def extract_books(self):
        """2. Extraire les livres avec toutes les informations"""
        logger.info("Extraction des livres...")
        
        page = 1
        while True:
            try:
                if page == 1:
                    url = f"{self.books_base_url}/catalogue/page-{page}.html"
                else:
                    url = f"{self.books_base_url}/catalogue/page-{page}.html"
                
                response = self.session.get(url)
                if response.status_code == 404:
                    break
                    
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Trouver tous les livres sur la page
                book_containers = soup.find_all('article', class_='product_pod')
                if not book_containers:
                    break
                
                for book_container in book_containers:
                    book_data = self.extract_book_details(book_container)
                    if book_data:
                        self.books.append(book_data)
                
                logger.info(f"Page {page}: {len(book_containers)} livres trouvés")
                page += 1
                time.sleep(0.5)  # Pause pour éviter de surcharger le serveur
                
                # Limiter pour les tests (retirer cette ligne en production)
                if page > 5:  # Limiter à 5 pages pour les tests
                    break
                    
            except Exception as e:
                logger.error(f"Erreur page {page}: {e}")
                break
        
        logger.info(f"Total: {len(self.books)} livres extraits")

    def extract_book_details(self, book_container):
        """Extraire les détails d'un livre"""
        try:
            # Titre
            title_element = book_container.find('h3').find('a')
            title = title_element.get('title', '') if title_element else ''
            
            # URL du livre pour plus de détails
            book_url = urljoin(self.books_base_url, title_element.get('href', ''))
            
            # Prix
            price_element = book_container.find('p', class_='price_color')
            price_text = price_element.text if price_element else '£0.00'
            price = float(re.sub(r'[£,]', '', price_text))
            
            # Disponibilité
            availability_element = book_container.find('p', class_='instock availability')
            availability = availability_element.text.strip() if availability_element else 'Unknown'
            
            # Note
            rating_element = book_container.find('p', class_=lambda x: x and 'star-rating' in x)
            rating = None
            if rating_element:
                rating_classes = rating_element.get('class', [])
                for cls in rating_classes:
                    if cls in ['One', 'Two', 'Three', 'Four', 'Five']:
                        rating = self.extract_rating_from_class(cls)
                        break
            
            # Image
            img_element = book_container.find('div', class_='image_container').find('img')
            image_url = urljoin(self.books_base_url, img_element.get('src', '')) if img_element else ''
            
            # Obtenir plus de détails depuis la page du livre
            book_details = self.get_book_page_details(book_url)
            
            return {
                'title': title,
                'price': price,
                'availability': availability,
                'rating': rating,
                'image_url': image_url,
                'url': book_url,
                **book_details
            }
            
        except Exception as e:
            logger.error(f"Erreur extraction livre: {e}")
            return None

    def get_book_page_details(self, book_url):
        """Obtenir les détails depuis la page individuelle du livre"""
        details = {
            'description': '',
            'upc': '',
            'publication_year': None,
            'category': ''
        }
        
        try:
            response = self.session.get(book_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Description
            desc_element = soup.find('div', id='product_description')
            if desc_element:
                desc_p = desc_element.find_next_sibling('p')
                if desc_p:
                    details['description'] = desc_p.text.strip()
            
            # Informations du tableau
            table = soup.find('table', class_='table table-striped')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        key = th.text.strip().lower()
                        value = td.text.strip()
                        
                        if 'upc' in key:
                            details['upc'] = value
                        elif 'available' in key:
                            # Essayer d'extraire une année de publication
                            year_match = re.search(r'20\d{2}', value)
                            if year_match:
                                details['publication_year'] = int(year_match.group())
                            else:
                                # Générer une année aléatoire entre 2000 et 2023
                                details['publication_year'] = random.randint(2000, 2023)
            
            # Catégorie depuis le breadcrumb
            breadcrumb = soup.find('ul', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) >= 3:  # Home > Books > Category
                    details['category'] = links[2].text.strip()
            
            time.sleep(0.2)  # Pause entre les requêtes
            
        except Exception as e:
            logger.error(f"Erreur détails livre {book_url}: {e}")
            # Générer une année par défaut si pas trouvée
            if not details['publication_year']:
                details['publication_year'] = random.randint(2000, 2023)
        
        return details

    def generate_publishers(self):
        """3. Générer les maisons d'édition fictives"""
        logger.info("Génération des maisons d'édition...")
        
        # Mélanger et assigner aléatoirement
        random.shuffle(self.fictional_publishers)
        self.publishers = [{'name': pub} for pub in self.fictional_publishers]
        
        logger.info(f"Générés {len(self.publishers)} éditeurs")

    def extract_authors_from_quotes(self):
        """4. Extraire les auteurs depuis quotes.toscrape.com"""
        logger.info("Extraction des auteurs depuis quotes.toscrape.com...")
        
        page = 1
        authors_dict = {}
        
        while True:
            try:
                if page == 1:
                    url = self.quotes_base_url
                else:
                    url = f"{self.quotes_base_url}/page/{page}/"
                
                response = self.session.get(url)
                if response.status_code == 404:
                    break
                    
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                quotes_containers = soup.find_all('div', class_='quote')
                if not quotes_containers:
                    break
                
                for quote_container in quotes_containers:
                    # Extraire auteur
                    author_element = quote_container.find('small', class_='author')
                    if author_element:
                        author_name = author_element.text.strip()
                        if author_name not in authors_dict:
                            authors_dict[author_name] = {
                                'name': author_name,
                                'birth_date': None,
                                'death_date': None,
                                'bio': '',
                                'openlibrary_key': None
                            }
                    
                    # Extraire la citation
                    text_element = quote_container.find('span', class_='text')
                    if text_element:
                        quote_text = text_element.text.strip().strip('"')
                        
                        # Tags
                        tags_elements = quote_container.find_all('a', class_='tag')
                        quote_tags = [tag.text.strip() for tag in tags_elements]
                        
                        self.quotes.append({
                            'text': quote_text,
                            'author': author_name,
                            'tags': quote_tags,
                            'book_source': None  # À compléter plus tard
                        })
                        
                        # Ajouter les tags à notre set
                        self.tags.update(quote_tags)
                
                logger.info(f"Page {page}: {len(quotes_containers)} citations trouvées")
                page += 1
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erreur page quotes {page}: {e}")
                break
        
        self.authors = list(authors_dict.values())
        logger.info(f"Total: {len(self.authors)} auteurs et {len(self.quotes)} citations")

    def enrich_authors_with_openlibrary(self):
        """Enrichir les auteurs avec l'API OpenLibrary"""
        logger.info("Enrichissement des auteurs avec OpenLibrary...")
        
        for i, author in enumerate(self.authors):
            try:
                # Recherche de l'auteur
                search_url = f"{self.openlibrary_api}/search/authors.json"
                params = {'q': author['name'], 'limit': 1}
                
                response = self.session.get(search_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('docs'):
                    author_info = data['docs'][0]
                    
                    # Mettre à jour les informations
                    if 'key' in author_info:
                        author['openlibrary_key'] = author_info['key']
                    
                    if 'birth_date' in author_info:
                        try:
                            birth_year = re.search(r'\d{4}', str(author_info['birth_date']))
                            if birth_year:
                                author['birth_date'] = f"{birth_year.group()}-01-01"
                        except:
                            pass
                    
                    if 'death_date' in author_info:
                        try:
                            death_year = re.search(r'\d{4}', str(author_info['death_date']))
                            if death_year:
                                author['death_date'] = f"{death_year.group()}-01-01"
                        except:
                            pass
                
                time.sleep(0.5)  # Respecter les limites de l'API
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Enrichi {i + 1}/{len(self.authors)} auteurs")
                    
            except Exception as e:
                logger.error(f"Erreur enrichissement auteur {author['name']}: {e}")
                continue

    def insert_categories(self):
        """8. Insérer les catégories"""
        logger.info("Insertion des catégories...")
        cursor = self.conn.cursor()
        
        try:
            for category in self.categories:
                cursor.execute(
                    "INSERT INTO categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                    (category['name'],)
                )
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM categories")
            count = cursor.fetchone()[0]
            logger.info(f"Catégories insérées: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion catégories: {e}")
        finally:
            cursor.close()

    def insert_publishers(self):
        """9. Insérer les maisons d'édition"""
        logger.info("Insertion des éditeurs...")
        cursor = self.conn.cursor()
        
        try:
            for publisher in self.publishers:
                cursor.execute(
                    "INSERT INTO publishers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                    (publisher['name'],)
                )
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM publishers")
            count = cursor.fetchone()[0]
            logger.info(f"Éditeurs insérés: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion éditeurs: {e}")
        finally:
            cursor.close()

    def insert_authors(self):
        """10. Insérer les auteurs"""
        logger.info("Insertion des auteurs...")
        cursor = self.conn.cursor()
        
        try:
            for author in self.authors:
                cursor.execute("""
                    INSERT INTO authors (name, birth_date, death_date, bio, openlibrary_key) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (name, birth_date) DO NOTHING
                """, (
                    author['name'],
                    author.get('birth_date'),
                    author.get('death_date'),
                    author.get('bio', ''),
                    author.get('openlibrary_key')
                ))
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM authors")
            count = cursor.fetchone()[0]
            logger.info(f"Auteurs insérés: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion auteurs: {e}")
        finally:
            cursor.close()

    def insert_books(self):
        """11. Insérer les livres"""
        logger.info("Insertion des livres...")
        cursor = self.conn.cursor()
        
        try:
            # Récupérer les IDs des catégories et éditeurs
            cursor.execute("SELECT id, name FROM categories")
            categories_map = {row[1]: row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT id, name FROM publishers")
            publishers_map = {row[1]: row[0] for row in cursor.fetchall()}
            publishers_list = list(publishers_map.keys())
            
            for book in self.books:
                # Trouver l'ID de la catégorie
                category_id = categories_map.get(book.get('category'))
                if not category_id and self.categories:
                    # Assigner une catégorie aléatoire si pas trouvée
                    random_category = random.choice(list(categories_map.keys()))
                    category_id = categories_map[random_category]
                
                # Assigner un éditeur aléatoire
                publisher_name = random.choice(publishers_list)
                publisher_id = publishers_map[publisher_name]
                
                cursor.execute("""
                    INSERT INTO books (title, price, availability, description, rating, 
                                     image_url, upc, publication_year, category_id, publisher_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    book['title'][:500],  # Limiter la longueur
                    book.get('price'),
                    book.get('availability', '')[:100],
                    book.get('description', ''),
                    book.get('rating'),
                    book.get('image_url', '')[:500],
                    book.get('upc', '')[:50],
                    book.get('publication_year'),
                    category_id,
                    publisher_id
                ))
                
                book_id = cursor.fetchone()[0]
                book['db_id'] = book_id
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM books")
            count = cursor.fetchone()[0]
            logger.info(f"Livres insérés: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion livres: {e}")
        finally:
            cursor.close()

    def insert_book_authors_relations(self):
        """12. Insérer les relations livre <-> auteur"""
        logger.info("Insertion des relations livre-auteur...")
        cursor = self.conn.cursor()
        
        try:
            # Récupérer les IDs des auteurs
            cursor.execute("SELECT id, name FROM authors")
            authors_map = {row[1]: row[0] for row in cursor.fetchall()}
            authors_list = list(authors_map.keys())
            
            # Associer 1-3 auteurs par livre
            for book in self.books:
                if 'db_id' not in book:
                    continue
                    
                # Choisir 1-3 auteurs aléatoirement
                num_authors = random.randint(1, min(3, len(authors_list)))
                selected_authors = random.sample(authors_list, num_authors)
                
                for author_name in selected_authors:
                    author_id = authors_map[author_name]
                    
                    cursor.execute("""
                        INSERT INTO book_authors (book_id, author_id)
                        VALUES (%s, %s)
                        ON CONFLICT (book_id, author_id) DO NOTHING
                    """, (book['db_id'], author_id))
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM book_authors")
            count = cursor.fetchone()[0]
            logger.info(f"Relations livre-auteur insérées: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion relations livre-auteur: {e}")
        finally:
            cursor.close()

    def insert_quotes(self):
        """13. Insérer les citations"""
        logger.info("Insertion des citations...")
        cursor = self.conn.cursor()
        
        try:
            # Récupérer les IDs des auteurs
            cursor.execute("SELECT id, name FROM authors")
            authors_map = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Récupérer quelques titres de livres pour associer aux citations
            cursor.execute("SELECT title FROM books LIMIT 50")
            book_titles = [row[0] for row in cursor.fetchall()]
            
            for quote in self.quotes:
                author_id = authors_map.get(quote['author'])
                
                # Parfois associer une source de livre
                book_source = None
                if random.random() < 0.3:  # 30% de chance d'avoir une source
                    book_source = random.choice(book_titles)
                
                cursor.execute("""
                    INSERT INTO quotes (text, author_id, book_source)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (quote['text'], author_id, book_source))
                
                quote_id = cursor.fetchone()[0]
                quote['db_id'] = quote_id
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM quotes")
            count = cursor.fetchone()[0]
            logger.info(f"Citations insérées: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion citations: {e}")
        finally:
            cursor.close()

    def insert_tags(self):
        """14. Insérer les tags"""
        logger.info("Insertion des tags...")
        cursor = self.conn.cursor()
        
        try:
            for tag in self.tags:
                cursor.execute(
                    "INSERT INTO tags (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                    (tag,)
                )
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM tags")
            count = cursor.fetchone()[0]
            logger.info(f"Tags insérés: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion tags: {e}")
        finally:
            cursor.close()

    def insert_quote_tags_relations(self):
        """15. Insérer les associations quote_tags"""
        logger.info("Insertion des relations citation-tag...")
        cursor = self.conn.cursor()
        
        try:
            # Récupérer les IDs des tags
            cursor.execute("SELECT id, name FROM tags")
            tags_map = {row[1]: row[0] for row in cursor.fetchall()}
            
            for quote in self.quotes:
                if 'db_id' not in quote:
                    continue
                    
                for tag_name in quote.get('tags', []):
                    tag_id = tags_map.get(tag_name)
                    if tag_id:
                        cursor.execute("""
                            INSERT INTO quote_tags (quote_id, tag_id)
                            VALUES (%s, %s)
                            ON CONFLICT (quote_id, tag_id) DO NOTHING
                        """, (quote['db_id'], tag_id))
            
            self.conn.commit()
            cursor.execute("SELECT COUNT(*) FROM quote_tags")
            count = cursor.fetchone()[0]
            logger.info(f"Relations citation-tag insérées: {count}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion relations citation-tag: {e}")
        finally:
            cursor.close()

    def display_summary(self):
        """16. Afficher le résumé des insertions"""
        logger.info("=== RÉSUMÉ DES INSERTIONS ===")
        cursor = self.conn.cursor()
        
        try:
            tables = ['categories', 'publishers', 'authors', 'books', 'quotes', 'tags', 'book_authors', 'quote_tags']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"{table.upper()}: {count} enregistrements")
            
        except Exception as e:
            logger.error(f"Erreur affichage résumé: {e}")
        finally:
            cursor.close()

    def verify_relations(self):
        """17. Vérifier les relations croisées via des requêtes SELECT"""
        logger.info("=== VÉRIFICATION DES RELATIONS ===")
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Requête 1: Livres avec leurs catégories et éditeurs
            cursor.execute("""
                SELECT b.title, c.name as category, p.name as publisher, b.price
                FROM books b
                JOIN categories c ON b.category_id = c.id
                JOIN publishers p ON b.publisher_id = p.id
                LIMIT 10
            """)
            
            logger.info("LIVRES avec catégories et éditeurs (10 premiers):")
            for row in cursor.fetchall():
                logger.info(f"  '{row['title']}' - {row['category']} - {row['publisher']} - £{row['price']}")
            
            # Requête 2: Auteurs avec leurs livres
            cursor.execute("""
                SELECT a.name as author, STRING_AGG(b.title, ', ') as books
                FROM authors a
                JOIN book_authors ba ON a.id = ba.author_id
                JOIN books b ON ba.book_id = b.id
                GROUP BY a.id, a.name
                LIMIT 10
            """)
            
            logger.info("\nAUTEURS avec leurs livres (10 premiers):")
            for row in cursor.fetchall():
                logger.info(f"  {row['author']}: {row['books'][:100]}...")
            
            # Requête 3: Citations avec auteurs et tags
            cursor.execute("""
                SELECT q.text, a.name as author, STRING_AGG(t.name, ', ') as tags
                FROM quotes q
                JOIN authors a ON q.author_id = a.id
                LEFT JOIN quote_tags qt ON q.id = qt.quote_id
                LEFT JOIN tags t ON qt.tag_id = t.id
                GROUP BY q.id, q.text, a.name
                LIMIT 5
            """)
            
            logger.info("\nCITATIONS avec auteurs et tags (5 premières):")
            for row in cursor.fetchall():
                logger.info(f"  \"{row['text'][:80]}...\" - {row['author']} - Tags: {row['tags'] or 'Aucun'}")
            
            # Requête 4: Statistiques générales
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT b.id) as total_books,
                    COUNT(DISTINCT a.id) as total_authors,
                    COUNT(DISTINCT q.id) as total_quotes,
                    AVG(b.price) as avg_price,
                    COUNT(DISTINCT c.id) as total_categories
                FROM books b
                CROSS JOIN authors a
                CROSS JOIN quotes q
                CROSS JOIN categories c
            """)
            
            stats = cursor.fetchone()
            logger.info(f"\nSTATISTIQUES:")
            logger.info(f"  Total livres: {stats['total_books']}")
            logger.info(f"  Total auteurs: {stats['total_authors']}")
            logger.info(f"  Total citations: {stats['total_quotes']}")
            logger.info(f"  Prix moyen: £{stats['avg_price']:.2f}")
            logger.info(f"  Total catégories: {stats['total_categories']}")
            
        except Exception as e:
            logger.error(f"Erreur vérification relations: {e}")
        finally:
            cursor.close()

    def export_to_csv_json(self):
        """18. Exporter certaines tables au format CSV ou JSON"""
        logger.info("Export des données...")
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Export des livres en CSV
            cursor.execute("""
                SELECT b.title, b.price, b.rating, c.name as category, 
                       p.name as publisher, b.publication_year
                FROM books b
                JOIN categories c ON b.category_id = c.id
                JOIN publishers p ON b.publisher_id = p.id
                ORDER BY b.title
            """)
            
            books_data = cursor.fetchall()
            with open('books_export.csv', 'w', newline='', encoding='utf-8') as csvfile:
                if books_data:
                    writer = csv.DictWriter(csvfile, fieldnames=books_data[0].keys())
                    writer.writeheader()
                    writer.writerows(books_data)
            
            logger.info(f"Livres exportés en CSV: {len(books_data)} enregistrements")
            
            # Export des citations en JSON
            cursor.execute("""
                SELECT q.text, a.name as author, q.book_source,
                       ARRAY_AGG(t.name) as tags
                FROM quotes q
                JOIN authors a ON q.author_id = a.id
                LEFT JOIN quote_tags qt ON q.id = qt.quote_id
                LEFT JOIN tags t ON qt.tag_id = t.id
                GROUP BY q.id, q.text, a.name, q.book_source
                ORDER BY a.name
            """)
            
            quotes_data = []
            for row in cursor.fetchall():
                quotes_data.append({
                    'text': row['text'],
                    'author': row['author'],
                    'book_source': row['book_source'],
                    'tags': [tag for tag in row['tags'] if tag]
                })
            
            with open('quotes_export.json', 'w', encoding='utf-8') as jsonfile:
                json.dump(quotes_data, jsonfile, ensure_ascii=False, indent=2)
            
            logger.info(f"Citations exportées en JSON: {len(quotes_data)} enregistrements")
            
            # Export des relations auteur-livre en CSV
            cursor.execute("""
                SELECT a.name as author, b.title as book, b.publication_year
                FROM authors a
                JOIN book_authors ba ON a.id = ba.author_id
                JOIN books b ON ba.book_id = b.id
                ORDER BY a.name, b.title
            """)
            
            relations_data = cursor.fetchall()
            with open('author_books_relations.csv', 'w', newline='', encoding='utf-8') as csvfile:
                if relations_data:
                    writer = csv.DictWriter(csvfile, fieldnames=relations_data[0].keys())
                    writer.writeheader()
                    writer.writerows(relations_data)
            
            logger.info(f"Relations auteur-livre exportées: {len(relations_data)} enregistrements")
            
        except Exception as e:
            logger.error(f"Erreur export: {e}")
        finally:
            cursor.close()

    def close_connection(self):
        """19. Fermer la connexion PostgreSQL"""
        if self.conn:
            self.conn.close()
            logger.info("Connexion PostgreSQL fermée")

    def run_full_pipeline(self):
        """Exécuter le pipeline complet"""
        start_time = datetime.now()
        logger.info("=== DÉBUT DU PIPELINE COMPLET ===")
        
        try:
            # Connexion et création des tables
            self.connect_db()
            self.create_tables()
            
            # 1-4: Extraction des données
            self.extract_categories()
            self.extract_books()
            self.generate_publishers()
            self.extract_authors_from_quotes()
            self.enrich_authors_with_openlibrary()
            
            # 8-15: Insertion des données
            self.insert_categories()
            self.insert_publishers()
            self.insert_authors()
            self.insert_books()
            self.insert_book_authors_relations()
            self.insert_quotes()
            self.insert_tags()
            self.insert_quote_tags_relations()
            
            # 16-18: Vérification et export
            self.display_summary()
            self.verify_relations()
            self.export_to_csv_json()
            
        except Exception as e:
            logger.error(f"Erreur dans le pipeline: {e}")
            if self.conn:
                self.conn.rollback()
        finally:
            # 19: Fermeture
            self.close_connection()
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"=== PIPELINE TERMINÉ en {duration} ===")


def main():
    """Fonction principale"""
    # Configuration de la db
    db_config = {
        'host': 'localhost',
        'database': 'books_db',
        'user': 'root',
        'password': 'example',
        'port': 5432
    }
    
    # Créer et exécuter le scraper
    scraper = BooksScraperDB(db_config)
    scraper.run_full_pipeline()


if __name__ == "__main__":
    main()

