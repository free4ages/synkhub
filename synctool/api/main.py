import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from ..core.models import SchedulerConfig
from ..scheduler.file_scheduler import FileBasedScheduler
from ..monitoring.metrics_storage import MetricsStorage
from .state import app_state


def create_scheduler_config(config_dir: str = "./configs", metrics_dir: str = "./data/metrics", logs_dir: str = "./data/logs"):
    """Create scheduler config with provided paths"""
    return SchedulerConfig(
        config_dir=config_dir,
        metrics_dir=metrics_dir,
        logs_dir=logs_dir
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logging.info("Starting Synctool Dashboard API")
    
    # Initialize components based on environment or config
    # Use default values if not set by main()
    config = app_state.get("scheduler_config") or create_scheduler_config()
    app_state["scheduler_config"] = config
    
    app_state["metrics_storage"] = MetricsStorage(
        metrics_dir=str(config.metrics_dir),
        max_runs_per_job=config.max_runs_per_job
    )
    
    logging.info(f"Using config directory: {config.config_dir}")
    logging.info(f"Using metrics directory: {config.metrics_dir}")
    
    yield
    
    # Shutdown
    logging.info("Shutting down Synctool Dashboard API")


# Create FastAPI app
app = FastAPI(
    title="Synctool Dashboard API",
    description="API for monitoring and managing Synctool sync jobs",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers (imported here to avoid circular import)
from .routers import jobs, runs
app.include_router(jobs.router)
app.include_router(runs.router)

# Include configure router
try:
    from .routers import configure
    app.include_router(configure.router)
except ImportError:
    # Configure router not available
    pass


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with basic dashboard info"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Synctool Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .header { color: #333; }
            .endpoint { margin: 10px 0; padding: 10px; background: #f5f5f5; }
            .method { font-weight: bold; color: #007acc; }
            .path { color: #666; }
        </style>
    </head>
    <body>
        <h1 class="header">Synctool Dashboard API</h1>
        <p>Welcome to the Synctool Dashboard API. This API provides endpoints for monitoring and managing sync jobs.</p>
        
        <h2>Available Endpoints:</h2>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/jobs/</span>
            <br>List all available sync jobs
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/jobs/{job_name}</span>
            <br>Get detailed information about a specific job
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/runs/{job_name}</span>
            <br>List all runs for a specific job
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/runs/{job_name}/{run_id}</span>
            <br>Get detailed information about a specific run
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/runs/{job_name}/{run_id}/logs</span>
            <br>Get logs for a specific run
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/runs/</span>
            <br>List runs from all jobs
        </div>
        
        <h2>Documentation:</h2>
        <p>
            <a href="/docs">Interactive API Documentation (Swagger UI)</a><br>
            <a href="/redoc">Alternative API Documentation (ReDoc)</a>
        </p>
    </body>
    </html>
    """
    return html_content


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "synctool-dashboard-api",
        "version": "1.0.0"
    }


@app.get("/api/status")
async def api_status():
    """Get API and system status"""
    try:
        metrics_storage = app_state.get("metrics_storage")
        if not metrics_storage:
            raise HTTPException(status_code=500, detail="Metrics storage not initialized")
        
        # Get basic statistics
        all_jobs = metrics_storage.get_all_jobs()
        total_jobs = len(all_jobs)
        
        # Count total runs across all jobs
        total_runs = 0
        active_runs = 0
        failed_runs = 0
        
        for job_name in all_jobs:
            job_runs = metrics_storage.get_job_runs(job_name, limit=100)  # Limit for performance
            total_runs += len(job_runs)
            active_runs += len([r for r in job_runs if r.status == "running"])
            failed_runs += len([r for r in job_runs if r.status == "failed"])
        
        return {
            "status": "operational",
            "statistics": {
                "total_jobs": total_jobs,
                "total_runs": total_runs,
                "active_runs": active_runs,
                "failed_runs": failed_runs
            },
            "config": {
                "metrics_dir": str(app_state.get("scheduler_config").metrics_dir) if app_state.get("scheduler_config") else "unknown",
                "max_runs_per_job": app_state.get("scheduler_config").max_runs_per_job if app_state.get("scheduler_config") else 50
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


def main():
    """Main function to run the API server"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Synctool Dashboard API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--config-dir", default="./configs", help="Directory containing job configs")
    parser.add_argument("--metrics-dir", default="./data/metrics", help="Directory for metrics storage")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create config with command line args before starting the app
    config = create_scheduler_config(
        config_dir=args.config_dir,
        metrics_dir=args.metrics_dir,
        logs_dir="./data/logs"
    )
    app_state["scheduler_config"] = config
    
    logging.info(f"Starting API server with config directory: {args.config_dir}")
    logging.info(f"Starting API server with metrics directory: {args.metrics_dir}")
    
    # Run the server
    uvicorn.run(
        "synctool.api.main:app",
        host=args.host,
        port=args.port,
        reload=False,  # Set to True for development
        log_level=args.log_level.lower()
    )


if __name__ == "__main__":
    main()
