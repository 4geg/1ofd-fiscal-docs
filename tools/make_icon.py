from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "assets" / "app.ico"
OUT.parent.mkdir(parents=True, exist_ok=True)

size = 256
img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.rounded_rectangle((14, 14, 242, 242), radius=70, fill=(0, 119, 182, 255))
d.ellipse((58, 58, 198, 198), fill=(202, 240, 248, 255))
try:
    font = ImageFont.truetype("arialbd.ttf", 96)
except Exception:
    font = ImageFont.load_default()
text = "1"
bbox = d.textbbox((0, 0), text, font=font)
tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
d.text(((size - tw) / 2, (size - th) / 2 - 8), text, font=font, fill=(3, 4, 94, 255))
img.save(OUT, sizes=[(16,16),(24,24),(32,32),(48,48),(64,64),(128,128),(256,256)])
print(OUT)
