def get_price_summary():
    return {
        "cheap": {
            "text": "쌀 10kg · 35,064원<br/>전주 대비 ▼0.2%",
            "bg": "#eaf2fb"
        },
        "expensive": {
            "text": "양파 1kg · 2,246원<br/>전주 대비 ▲2.1%",
            "bg": "#fff8e1"
        },
        "suggest": {
            "text": "쌀 10kg · 35,064원",
            "bg": "#eaf7ea"
        }
    }

def get_popular_items():
    return [
        "당근 3,128원",
        "멜론 11,160원",
        "포도 12,420원",
        "무 2,501원",
    ]
