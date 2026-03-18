import os
import csv
import json
import io
import requests
import uuid

from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime,timezone

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

load_dotenv()

API_KEY = os.getenv("RAPIDAPI_KEY")
if not API_KEY:
    raise RuntimeError("RAPIDAPI_KEY is not set. Put it in your .env file.")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Supabase client initialized.\n")


# TikTok API config

API_URL = "https://tiktok-shop-analysis.p.rapidapi.com/product/top/trending"
API_HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "tiktok-shop-analysis.p.rapidapi.com",
}

# category keywords
BUCKET_KEYWORDS = {
    "kitchen","kitchen dining","cookware","bakeware","utensil","...","air fryer","blender","toaster","microwave","pressure cooker",
    "health","wellness","vitamin","supplement","hygiene","first ...,","personal care","oral care","skin care","skincare","hair care",
    "fitness","sport","sports","outdoor","gym","workout","exerci...s","dumbbell","kettlebell","treadmill","bike","resistance band",
    "home","home supplies","home improvement","home garden","gar...","cleaning","storage","organization","tool","hardware","lighting"
}

# csv
CSV_FIELDS = [
    "product_id",
    "product_name",
    "price",
    "currency",
    "sales",
    "category",
    "categories",
    "shop_name",
    "shop_id",
    "product_url",
    "image_url",
]


# helper methods 

def normalize_text(s: str) -> str:
    return (s or "").lower().replace("&", " ").replace("/", " ").replace("-", " ")


def iter_products(payload):
    if isinstance(payload, list):
        for p in payload:
            if isinstance(p, dict):
                yield p
        return

    if isinstance(payload, dict):
        d = payload.get("data")
        if isinstance(d, list):
            for p in d:
                if isinstance(p, dict):
                    yield p
            return
        if isinstance(d, dict):
            for key in ("list", "items", "products", "result"):
                lst = d.get(key)
                if isinstance(lst, list):
                    for p in lst:
                        if isinstance(p, dict):
                            yield p
                    return
        for key in ("list", "items", "products", "result"):
            lst = payload.get(key)
            if isinstance(lst, list):
                for p in lst:
                    if isinstance(p, dict):
                        yield p
                return


def to_text(x) -> str:
    return "" if x is None else str(x)


def flatten_categories(p: dict) -> str:
    cats = p.get("categories")
    items = []
    if isinstance(cats, list):
        for c in cats:
            if isinstance(c, str):
                items.append(c)
            elif isinstance(c, dict):
                for k in ("name", "title", "category_name", "label"):
                    if isinstance(c.get(k), str):
                        items.append(c[k])
                        break
    if isinstance(p.get("category"), str):
        items.append(p["category"])
    return "; ".join(items)


def collect_text(p: dict) -> str:
    name = p.get("product_name") or p.get("title") or p.get("name") or ""
    blob = [name, flatten_categories(p)]
    return normalize_text(" ".join(blob))


def is_target(p: dict) -> bool:
    haystack = collect_text(p)
    for kw in BUCKET_KEYWORDS:
        if kw in haystack:
            return True
    return False


def extract_price_and_currency(p: dict):
    money = p.get("price") or p.get("min_price") or p.get("price_info")
    if isinstance(money, dict):
        for k in ("price", "value", "amount", "min_price", "max_price"):
            if k in money:
                return to_text(money.get(k)), to_text(money.get("currency") or p.get("currency"))
        return "", to_text(money.get("currency") or p.get("currency"))
    for k in ("price", "min_price", "max_price", "sale_price"):
        if k in p:
            return to_text(p.get(k)), to_text(p.get("currency"))
    return "", to_text(p.get("currency"))


def extract_sales(p: dict):
    for k in ("sales", "sold", "sold_count", "sales_count", "order_count"):
        if k in p:
            return to_text(p.get(k))
    return ""


def product_row(p: dict) -> dict:
    price, currency = extract_price_and_currency(p)
    return {
        "product_id": to_text(p.get("product_id") or p.get("id")),
        "product_name": to_text(p.get("product_name") or p.get("title") or p.get("name")),
        "price": price,
        "currency": currency,
        "sales": extract_sales(p),
        "category": "",  # CSV-only convenience; Supabase uses a different category mapping
        "categories": flatten_categories(p),
        "shop_name": to_text(p.get("shop_info", {}).get("name") if isinstance(p.get("shop_info"), dict) else ""),
        "shop_id": to_text(p.get("shop_info", {}).get("shop_id") if isinstance(p.get("shop_info"), dict) else ""),
        "product_url": to_text(p.get("product_url") or p.get("detail_url") or p.get("url")),
        "image_url": to_text(p.get("cover") or p.get("image") or p.get("img")),
    }


def fetch_page(page: int, pagesize: int):
    qs = {
        "region": "US",
        "order": "2,2",
        "pagesize": str(pagesize),
        "page": str(page),
    }
    r = requests.get(API_URL, headers=API_HEADERS, params=qs, timeout=30)
    r.raise_for_status()
    return list(iter_products(r.json()))


def fetch_and_filter(pages: int = 5, pagesize: int = 50):
    print(f"Fetching TikTok products: pages={pages}, pagesize={pagesize}")
    all_products = []
    for page in range(1, pages + 1):
        page_products = fetch_page(page, pagesize)
        print(f"  Page {page}: {len(page_products)} products")
        all_products.extend(page_products)

    # Deduplicate by product_id / id
    seen = set()
    deduped = []
    for p in all_products:
        pid = to_text(p.get("product_id") or p.get("id"))
        if pid and pid not in seen:
            seen.add(pid)
            deduped.append(p)

    # Bucket filter
    filtered = [p for p in deduped if is_target(p)]
    print(f"Fetched={len(deduped)} after dedupe, Kept={len(filtered)} after bucket filter")
    return filtered

#This function should go to the API creators URL and check when the API was last updated
def last_updated():
    CREATOR_URL = "https://rapidapi.com/user/hoangdaicntt"
    try:
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(30)
        driver.get(CREATOR_URL)
        updated = driver.find_element(By.XPATH, "//span[contains(text(),'Updated')]")

        updated_time = updated.text.strip()
        updated_time = updated_time[8:len(updated_time)-5]

        if "year" in updated_time:
            approx_days = 365 * int(updated_time[0:updated_time.find(" ")])
        elif "month" in updated_time:
            approx_days = 30 * int(updated_time[0:updated_time.find(" ")])
        elif "week" in updated_time:
            approx_days = 7 * int(updated_time[0:updated_time.find(" ")])
        elif "day" in updated_time:
            approx_days = int(updated_time[0:updated_time.find(" ")])
        else:
            approx_days = 0
            
        if approx_days < 7:
            return f"Recently updated, and {check_methods()}"
        elif approx_days > 180:
            return "API has not been updated in over 6 months; it may have been abandoned"
        else:
            return "API has been updated in the past 6 months; it may need to be updated. Try program again later"

    except Exception as e:
        return f"{CREATOR_URL} could not be opened. Please check that you are connected to the internet, and if this continues to fail, check that the user still exists on RapidAPI."

#This function should go to the API and confirm that the List Top Products method is still present
def check_methods():
    API_URL = "https://rapidapi.com/hoangdaicntt/api/tiktok-shop-analysis"
    try:
        driver = webdriver.Chrome()
        driver.implicitly_wait(30)
        driver.get(API_URL)
        updated = " "
        updated = driver.find_element(By.XPATH, "//div[normalize-space()='List Top Products']")

        if updated.text == "List Top Products":
            return f"{updated.text} is still listed as a method of the API"
        return "the List Top Products method is not found"

    except Exception as e:
        return f"{API_URL} could not be opened. Please check that you are connected to the internet, and if this continues to fail, check that the user still exists on RapidAPI."

def save_csv_and_json(filtered_products, csv_path="tiktok_products_filtered.csv", json_path="tiktok_products_filtered.json"):
    # CSV
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for p in filtered_products:
            writer.writerow(product_row(p))

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(filtered_products, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(filtered_products)} products to:")
    print(f"  {csv_path}")
    print(f"  {json_path}")

def get_utc_now() -> str:
    ''' Returns current UTC time in ISO 8601 format '''
    return datetime.now(timezone.utc).isoformat()

def start_pipeline_run(note: str="auto run", status: str="started") -> str:
    ''' Creates a new pipeline run entry in Supabase and returns its ID '''
    run_id = str(uuid.uuid4())
    record = {
        "run_id": run_id,
        "note": note,
        "status": status,
        "started_at": get_utc_now(),
    }
    try:
        supabase.table("pipeline_runs").insert(record).execute()
        print(f"Pipeline run started with ID: {run_id}")
    except Exception as e:
        print(f"Error starting pipeline run: {e}")
    return run_id

def complete_pipeline_run(run_id: str, status: str="completed", note: str="", error: str=""):
    ''' Updates an existing pipeline run entry in Supabase to mark it as completed '''
    payload = {
        "run_id": run_id,
        "status": status,
        "finished_at": get_utc_now(),
    }
    if note:
        payload["note"] = note
    try:
        supabase.table("pipeline_runs").update(payload).eq("run_id", run_id).execute()
        print(f"Pipeline run completed with ID: {run_id}")
    except Exception as e:
        print(f"Error completing pipeline run: {e}")

# Supabase upsert


def upsert_products_to_supabase(products, run_id: str, table_name: str = "tiktok_products"):
    """
    Map actual TikTok JSON fields to your Supabase table columns.

    Supabase columns:
      title            <- product_name (or title/name)
      category         <- category (or first of categories[])
      cover            <- cover_url
      shop_name        <- seller.seller_name
      total_sold_count  <- int(total_sale_cnt)
      recent_sold_count <- int(sale_cnt)
      sale_amount      <- float(total_sale_gmv_amt)
      price            <- float(avg_price / min_price / max_price)
      commission_rate  <- float(commission) if present
    """


    def parse_money(val):
        if val in (None, "", "-"):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                cleaned = val.replace("$", "").replace(",", "").strip()
                return float(cleaned)
            except ValueError:
                return None
        return None

    products_to_upsert = []

    for product in products:
        # ---- TITLE ----
        raw_title = (
            product.get("title")
            or product.get("product_name")
            or product.get("name")
        )

        # ---- CATEGORY ----
        category = product.get("category")
        if not category:
            cats = product.get("categories")
            if isinstance(cats, list) and cats:
                category = cats[0]

        # ---- COVER URL ----
        cover = product.get("cover_url")

        # ---- SHOP NAME ----
        shop_name = None
        seller = product.get("seller")
        if isinstance(seller, dict):
            shop_name = seller.get("seller_name")

        # ---- METRICS ----
        recent_sold_count = parse_int(product.get("sale_cnt"))
        total_sold_count = parse_int(product.get("total_sale_cnt"))
        sale_amount = parse_money(product.get("total_sale_gmv_amt"))

        price_source = (
            product.get("avg_price")
            or product.get("min_price")
            or product.get("max_price")
        )
        price = parse_money(price_source)

        commission_rate = parse_money(product.get("commission"))

        new_product = {
            "title": raw_title,
            "category": category,
            "cover": cover,
            "shop_name": shop_name,
            "total_sold_count": total_sold_count,
            "recent_sold_count": recent_sold_count,
            "sale_amount": sale_amount,
            "price": price,
            "commission_rate": commission_rate,
            "run_id": run_id, # link to the current pipeline run
        }

        if new_product["title"]:
            products_to_upsert.append(new_product)

    if not products_to_upsert:
        print("No valid products to upsert into Supabase.")
        return

    # DEBUG: see one mapped row
    print("\nSample mapped product for upsert:")
    print(products_to_upsert[0])

    print(f"\nUpserting {len(products_to_upsert)} products into '{table_name}'...")
    try:
        response = supabase.table(table_name).upsert(
            products_to_upsert,
            on_conflict="title",  # keep this as-is
        ).execute()
        print(f"Supabase upsert complete: {len(response.data)} rows.")
    except Exception as e:
        print(f"Error during Supabase upsert: {e}")


def insert_rank_snapshot(products,run_id: str,top_k: int=10, table_name: str = "tiktok_rank_snapshots"):
    ''' Inserts a daily ranking snapshot for the top K products into tiktok_rank_snapshots table
        We store what we are about to upsert into tiktok_products, but as a historical snapshot.
        values are cleaned via parse_money / parse_int to match numeric columns
    '''
    if not products:
        print("No products provided for rank snapshot insertion.")
        return
    
    captured_at = get_utc_now()
    top_k = min(top_k, len(products))
    snapshots = []
    
    for rank, product in enumerate(products[:top_k], start=1):
        title = (
            product.get("title")
            or product.get("product_name")
            or product.get("name")
        )
        if not title:
            continue    
        category = product.get("category")
        if not category:    
            cats = product.get("categories")
            if isinstance(cats, list) and cats:
                category = cats[0]
        cover = product.get("cover_url")
        shop_name = None  
        seller = product.get("seller")
        if isinstance(seller, dict):
            shop_name = seller.get("seller_name")     
        
        product_id=(
            product.get("product_id")
            or product.get("id")
            or product.get("goods_id")
        )        
        
        snapshot = {
            "run_id": run_id,
            "top_k": top_k,
            "rank": rank,
            "product_id": str(product_id) if product_id is not None else None,
            "title": title,
            "category": category,
            "cover": cover,
            "shop_name": shop_name,
            "captured_at": captured_at,
            "total_sold_count": parse_int(product.get("total_sale_cnt")),
            "recent_sold_count": parse_int(product.get("sale_cnt")),
            "sale_amount": parse_money(product.get("total_sale_gmv_amt")),
            "price": parse_money(
                product.get("avg_price")
                or product.get("min_price")
                or product.get("max_price")
            ),
            "commission_rate": parse_money(product.get("commission")),
            
            
        }    
        snapshots.append(snapshot)

    if not snapshots:
        print("No valid snapshots to insert.")
        return

    print(f"Inserting {len(snapshots)} rank snapshots into '{table_name}'...")
    
    try:
        response = supabase.table(table_name).insert(snapshots).execute()
        print(f"Supabase inserted: {len(response.data)} rows.")
    except Exception as e:
        print(f"Error during Supabase insert: {e}")
        raise



def read_sorted_by_sold_count(table_name: str = "tiktok_products"):
    print("\nReading products sorted by recent_sold_count (desc)...")
    try:
        response = (
            supabase.table(table_name)
            .select("*")
            .order("recent_sold_count", desc=True)
            .execute()
        )
        data = response.data
        if data:
            print(f"Retrieved {len(data)} rows from Supabase.")
            return data
        else:
            print("No data found in Supabase table.")
            return []
    except Exception as e:
        print(f"Error reading from Supabase: {e}")
        return []


# PDF generation

def generate_pdf_report(data, filename: str = "tiktok_products_report.pdf"):
    """
    Simple PDF:
    - Title
    - For each product: image on the left, details on the right
    """
    print(f"\nGenerating PDF report: {filename}")
    try:
        doc = SimpleDocTemplate(
            filename,
            pagesize=letter,
            rightMargin=inch / 2,
            leftMargin=inch / 2,
            topMargin=inch / 2,
            bottomMargin=inch / 2,
        )

        styles = getSampleStyleSheet()
        story = []

        # Title
        title_style = styles["Title"]
        title_style.alignment = 1  # center
        story.append(Paragraph("TikTok Products (Sorted by Most Sold)", title_style))
        story.append(Spacer(1, 0.3 * inch))

        max_items = min(len(data), 50)

        for i, product in enumerate(data[:max_items], start=1):
            title = product.get("title") or "Untitled"
            category = product.get("category") or "N/A"
            cover = product.get("cover")
            shop_name = product.get("shop_name") or "N/A"
            price = product.get("price") or "N/A"
            commission = product.get("commission_rate") or "N/A"
            sold_count = product.get("recent_sold_count") or "N/A"
            total_sold_count = product.get("total_sold_count") or "N/A"
            sale_amount = product.get("sale_amount") or "N/A"

            # --- Image cell ---
            img_cell = Paragraph("No image", styles["BodyText"])
            if cover:
                try:
                    resp = requests.get(cover, timeout=10)
                    resp.raise_for_status()
                    image_data = io.BytesIO(resp.content)
                    img = Image(image_data, width=1.5 * inch, height=1.5 * inch)
                    img.hAlign = "CENTER"
                    img_cell = img
                except Exception as e:
                    print(f"Warning: Could not load image for '{title[:30]}...': {e}")

            commission_line = ""
            if commission not in (None, "", "N/A"):
                commission_line = f"&nbsp;&nbsp;|&nbsp;&nbsp; <b>Commission:</b> {commission}<br/>"

            # --- Text cell ---
            text_html = f"""
                <b>{i}. {title}</b><br/>
                <font size="9" color="grey">Category: {category}</font><br/><br/>
                <b>Shop:</b> {shop_name}<br/>
                <b>Price:</b> {price}<br/>
                <b>Units Recently Sold:</b> {sold_count} &nbsp;&nbsp;|&nbsp;&nbsp; <b>Total Revenue:</b> {sale_amount}<br/>
                <b>Total Units Sold:</b> {total_sold_count}
            """
            text_para = Paragraph(text_html, styles["BodyText"])

            table_data = [[img_cell, text_para]]
            table = Table(table_data, colWidths=[1.7 * inch, 4.8 * inch])
            table.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("BOX", (0, 0), (-1, -1), 0.5, colors.grey),
                        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                        ("LEFTPADDING", (0, 0), (-1, -1), 6),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]
                )
            )

            story.append(table)
            story.append(Spacer(1, 0.2 * inch))

        doc.build(story)
        print(f"PDF generated successfully: {filename}")
    except Exception as e:
        print(f"Error generating PDF: {e}")

def parse_int(val):
    if val in (None, "", "-"):
        return 0
    try:
        return int(str(val).replace("K", "0").replace(".", "")) if isinstance(val, str) and "K" in val else int(val)
    except (ValueError, TypeError):
        # Fallback: try to strip non-digits
        digits = "".join(ch for ch in str(val) if ch.isdigit())
        return int(digits) if digits else 0


def parse_money(val):
    if val in (None, "", "-"):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            cleaned = val.replace("$", "").replace(",", "").strip()
            #handle prcent like 15%-> 15.0 so Supabase numeric insert won't fail
            if cleaned.endswith("%"):
                cleaned = cleaned[:-1].strip()               
            if cleaned.endswith("K"):
                return float(cleaned[:-1]) * 1000.0
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


def pick_top_products(products, top_n=10, min_sales=0, use_recent=False):
    scored = []

    for p in products:
        if use_recent:
            sales = parse_int(p.get("total_sale_7d_cnt") or p.get("total_sale_1d_cnt"))
        else:
            sales = parse_int(p.get("sale_cnt"))

        revenue = parse_money(p.get("total_sale_gmv_amt"))

        if sales < min_sales:
            continue

        scored.append((sales, revenue, p))

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)

    top = [p for (_, _, p) in scored[:top_n]]
    print(f"Selected {len(top)} top products (min_sales={min_sales}, top_n={top_n})")
    return top

def has_valid_metrics(p: dict) -> bool:
    sold = p.get("sale_cnt")
    revenue = p.get("total_sale_gmv_amt")

    if not sold or str(sold).strip() in ("", "-"):
        return False
    if not revenue or str(revenue).strip() in ("", "-"):
        return False

    return True

def map_product_for_pdf(product: dict) -> dict:
    # Title
    raw_title = (
        product.get("title")
        or product.get("product_name")
        or product.get("name")
    )

    # Category
    category = product.get("category")
    if not category:
        cats = product.get("categories")
        if isinstance(cats, list) and cats:
            category = cats[0]

    # Cover URL
    cover = product.get("cover_url")

    # Shop name
    shop_name = None
    seller = product.get("seller")
    if isinstance(seller, dict):
        shop_name = seller.get("seller_name")

    # Metrics (strings are fine here, PDF just shows them)
    recent_sold_count = product.get("sale_cnt")
    total_sold_count = product.get("total_sale_cnt")
    sale_amount = product.get("total_sale_gmv_amt")

    price_source = (
        product.get("avg_price")
        or product.get("min_price")
        or product.get("max_price")
    )
    price = price_source

    commission_rate = product.get("commission")

    return {
        "title": raw_title,
        "category": category,
        "cover": cover,
        "shop_name": shop_name,
        "recent_sold_count": recent_sold_count,
        "total_sold_count": total_sold_count,
        "sale_amount": sale_amount,
        "price": price,
        "commission_rate": commission_rate,
    }

# Main 

def main():
    run_id = None
    try:
        # 0)Start pipeline run
        run_id = start_pipeline_run(note="Week2 daily run",status="started")
       
        # 1) Fetch + filter from TikTok
        filtered_products = fetch_and_filter(pages=5, pagesize=50)
        if not filtered_products:
            complete_pipeline_run(run_id, status="completed", note="No products after filtering")    
            print("No products after filtering – exiting.")
            return

        # 2) Pick only the best-performing products
        top_products = pick_top_products(
            filtered_products,
            top_n=10,
            min_sales=5,       # tweak this threshold
            use_recent=False,  # change to True to rank by recent sales
        )

        if not top_products:
            complete_pipeline_run(run_id, status="completed", note="No products passed top-product filter")
            print("No products passed the top-product filter – looking for reason.")
            print(last_updated())
            return


        # 3) Save CSV + JSON just for the cleaned top products
        save_csv_and_json(top_products)
         # 4) Insert daily snapshots(history)
        try:
            insert_rank_snapshot(top_products,run_id=run_id,top_k=10)
        except Exception as e:
            print(f"Warning: snapshot insert failed,continue pipeline. Error: {e}")
        # 5) Upsert into Supabase using the cleaned list ,passing run_id
        upsert_products_to_supabase(top_products, run_id=run_id)
        
        # 6) Build PDF rows directly from THIS RUN's cleaned products
        pdf_rows = [map_product_for_pdf(p) for p in top_products]

        # 7) Generate PDF from current response only
        generate_pdf_report(pdf_rows, filename="tiktok_products_report.pdf")
        complete_pipeline_run(run_id, status="completed", note=f"Upserted {len(top_products)} products")
    except Exception as e:
        print(f"Error in pipeline: {e}")
        if run_id:
            complete_pipeline_run(run_id, status="failed", note="Pipeline failed", error=str(e))    
        raise        


if __name__ == "__main__":
    main()


