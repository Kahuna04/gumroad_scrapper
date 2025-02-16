import asyncio
import json
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from src.script import ChineseNewsScraper, NEWS_SOURCES

app = FastAPI()

@app.get("/scrape-news")
async def scrape_news():
    try:
        # Initialize the scraper
        scraper = ChineseNewsScraper(max_retries=5, delay=2)
        
        # Run the scraper
        df = await scraper.scrape_all_sources()
        
        # Convert DataFrame to a list of dictionaries
        articles_list = df.to_dict(orient='records')
        
        # Return the JSON response with proper indentation
        return JSONResponse(content=articles_list, media_type="application/json")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
