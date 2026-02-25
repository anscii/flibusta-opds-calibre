# -*- coding: utf-8 -*-
from calibre.gui2.store.basic_config import BasicStoreConfig
from calibre.gui2.store.opensearch_store import OpenSearchOPDSStore
from calibre.gui2.store.search_result import SearchResult

class FlibustaStore(BasicStoreConfig, OpenSearchOPDSStore):

    name = 'Флибуста'
    open_search_url = 'https://flub.flibusta.is/opds/search'
    web_url = 'https://flibusta.is/'

    def get_opensearch_description_url(self):
        return None

    def _make_browser(self):
        from calibre import browser
        br = browser()
        br.set_handle_robots(False)
        br.addheaders = [('User-Agent', 'Calibre')]
        return br

    def _parse_feed(self, raw_data):
        try:
            from lxml import etree
        except ImportError:
            import xml.etree.ElementTree as etree

        if isinstance(raw_data, bytes):
            try:
                raw_data = raw_data.decode('utf-8')
            except UnicodeDecodeError:
                raw_data = raw_data.decode('cp1251', errors='replace')

        return etree.fromstring(raw_data.encode('utf-8') if isinstance(raw_data, str) else raw_data)

    def _fetch_url(self, br, url, timeout):
        response = br.open_novisit(url, timeout=timeout)
        return response.read()

    def _entries_to_results(self, root, ns):
        """Parse OPDS feed entries into SearchResult objects. Yields (SearchResult, is_book)."""
        import re
        for entry in root.xpath('//atom:entry', namespaces=ns):
            # Detect if this entry is an author catalog link (not a book)
            links = entry.xpath('.//atom:link', namespaces=ns)
            is_author_entry = False
            author_catalog_url = None
            for link in links:
                href = link.get('href', '')
                link_type = link.get('type', '')
                if 'opds-catalog' in link_type and '/a/' in href:
                    is_author_entry = True
                    author_catalog_url = ('https://flub.flibusta.is' + href
                                          if href.startswith('/') else href)
                    break

            if is_author_entry:
                yield None, author_catalog_url
                continue

            s = SearchResult()

            title_elem = entry.xpath('.//atom:title', namespaces=ns)
            s.title = title_elem[0].text.strip() if title_elem and title_elem[0].text else 'Unknown Title'

            author_elem = entry.xpath('.//atom:author/atom:name', namespaces=ns)
            if not author_elem:
                author_elem = entry.xpath('.//dc:creator', namespaces=ns)
            s.author = author_elem[0].text.strip() if author_elem and author_elem[0].text else 'Unknown Author'

            id_elem = entry.xpath('.//atom:id', namespaces=ns)
            book_id = None
            if id_elem and id_elem[0].text:
                id_match = re.search(r'(\d+)', id_elem[0].text)
                if id_match:
                    book_id = id_match.group(1)

            s.downloads = {}
            for link in links:
                href = link.get('href')
                rel = link.get('rel', '')
                link_type = link.get('type', '')
                title_attr = link.get('title', '')

                if not href:
                    continue
                if href.startswith('/'):
                    href = 'https://flub.flibusta.is' + href
                elif not href.startswith('http'):
                    href = 'https://flub.flibusta.is/' + href

                if rel == 'alternate' and ('html' in link_type or not link_type):
                    s.detail_item = href
                elif 'acquisition' in rel or 'download' in rel.lower():
                    fmt = None
                    combined = (link_type + href + title_attr).lower()
                    if 'fb2' in combined:
                        fmt = 'FB2'
                    elif 'epub' in combined:
                        fmt = 'EPUB'
                    elif 'mobi' in combined:
                        fmt = 'MOBI'
                    elif 'pdf' in combined:
                        fmt = 'PDF'
                    elif 'txt' in combined:
                        fmt = 'TXT'
                    if fmt:
                        s.downloads[fmt] = href

            if not s.downloads and book_id:
                base_url = 'https://flub.flibusta.is/b/' + book_id
                s.downloads = {'FB2': base_url + '/fb2',
                               'EPUB': base_url + '/epub',
                               'MOBI': base_url + '/mobi'}

            if not getattr(s, 'detail_item', None) and book_id:
                s.detail_item = 'https://flibusta.is/b/' + book_id

            summary_elem = entry.xpath('.//atom:summary', namespaces=ns)
            if not summary_elem:
                summary_elem = entry.xpath('.//atom:content', namespaces=ns)
            if summary_elem and summary_elem[0].text:
                s.comments = summary_elem[0].text.strip()

            date_elem = entry.xpath('.//atom:published', namespaces=ns)
            if not date_elem:
                date_elem = entry.xpath('.//atom:updated', namespaces=ns)
            if date_elem and date_elem[0].text:
                try:
                    from datetime import datetime
                    date_str = date_elem[0].text
                    if 'T' in date_str:
                        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    else:
                        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
                    s.pubdate = dt
                except Exception:
                    pass

            s.price = '$0.00'
            s.drm = SearchResult.DRM_UNLOCKED
            s.store_name = 'Флибуста'

            if s.title != 'Unknown Title' or s.author != 'Unknown Author':
                yield s, None

    def search(self, query, max_results=10, timeout=60):
        from urllib.parse import quote_plus

        ns = {'atom': 'http://www.w3.org/2005/Atom',
              'dc': 'http://purl.org/dc/terms/',
              'opds': 'http://opds-spec.org/2010/catalog'}

        try:
            br = self._make_browser()
            seen = set()  # deduplicate by (title, author)
            count = 0

            # --- 1. Search by book title ---
            books_url = '{}?searchTerm={}&searchType=books'.format(
                self.open_search_url, quote_plus(query))
            root = self._parse_feed(self._fetch_url(br, books_url, timeout))
            for s, _ in self._entries_to_results(root, ns):
                if s is None or count >= max_results:
                    continue
                key = (s.title.lower(), s.author.lower())
                if key not in seen:
                    seen.add(key)
                    count += 1
                    yield s

            if count >= max_results:
                return

            # --- 2. Search by author name ---
            authors_url = '{}?searchTerm={}&searchType=authors'.format(
                self.open_search_url, quote_plus(query))
            root = self._parse_feed(self._fetch_url(br, authors_url, timeout))

            # Collect author catalog URLs (top 3 to avoid too many requests)
            author_catalog_urls = []
            for s, catalog_url in self._entries_to_results(root, ns):
                if catalog_url:
                    author_catalog_urls.append(catalog_url)
                    if len(author_catalog_urls) >= 3:
                        break

            # Fetch books for each found author
            for catalog_url in author_catalog_urls:
                if count >= max_results:
                    break
                try:
                    author_root = self._parse_feed(self._fetch_url(br, catalog_url, timeout))
                    # Author catalog may have sub-sections; try to find "all books" link
                    all_books_url = None
                    for link in author_root.xpath('//atom:entry/atom:link', namespaces=ns):
                        href = link.get('href', '')
                        title_elem = link.getparent().xpath('.//atom:title', namespaces=ns)
                        title_text = title_elem[0].text if title_elem and title_elem[0].text else ''
                        if '/ab/' in href or 'все книги' in title_text.lower() or 'all' in title_text.lower():
                            all_books_url = ('https://flub.flibusta.is' + href
                                             if href.startswith('/') else href)
                            break

                    books_root = (self._parse_feed(self._fetch_url(br, all_books_url, timeout))
                                  if all_books_url else author_root)

                    for s, _ in self._entries_to_results(books_root, ns):
                        if s is None or count >= max_results:
                            continue
                        key = (s.title.lower(), s.author.lower())
                        if key not in seen:
                            seen.add(key)
                            count += 1
                            yield s
                except Exception as e:
                    import traceback
                    self.log.error('Error fetching author catalog %s: %s\n%s'
                                   % (catalog_url, e, traceback.format_exc()))

        except Exception as e:
            import traceback
            self.log.error('Error searching flibusta.is: %s\n%s' % (e, traceback.format_exc()))

    def get_details(self, search_result, timeout):
        search_result.drm = SearchResult.DRM_UNLOCKED

        if not search_result.downloads and search_result.detail_item:
            try:
                book_id = search_result.detail_item.split('/')[-1]
                base = 'https://flub.flibusta.is/b/'
                search_result.downloads = {
                    'FB2': base + book_id + '/fb2',
                    'EPUB': base + book_id + '/epub',
                    'MOBI': base + book_id + '/mobi',
                }
            except Exception:
                pass

        formats = list(search_result.downloads.keys())
        search_result.formats = ', '.join(formats) if formats else 'FB2, EPUB, MOBI'
        return True
