from fastapi import HTTPException
from fastapi.testclient import TestClient
from fastapi import FastAPI
from fastapi_celery.routers.api_healthcheck import router as healthcheck_router

# Create a test FastAPI app and include the healthcheck router
app = FastAPI()
app.include_router(healthcheck_router)

client = TestClient(app)


def test_api_health() -> None:
    """
    Test the /healthz endpoint for a successful health check response.

    Asserts:
        - Status code is 200.
        - Response JSON is {"status": "ok"}.
    """
    # Test the health check route for a successful response
    response = client.get("/api_health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_api_health_error_handling(monkeypatch) -> None:
    """
    Test error handling in the /healthz endpoint by simulating a failure in the internal health check.

    Uses:
        monkeypatch: Pytest fixture to override the behavior of _internal_health_check temporarily.

    Simulates:
        - An HTTPException with status code 503 and a custom detail message.

    Asserts:
        - Response status code is 503.
        - Response JSON contains:
            - "status": "error"
            - "details" includes the simulated error message.
    """
    # Simulate an error in the health check endpoint to test error handling
    def mock_health_check_error():
        raise HTTPException(
            status_code=503,
            detail="Simulated error"
        )

    # Patch the healthz function to simulate the error
    monkeypatch.setattr(
        "fastapi_celery.routers.api_healthcheck._internal_health_check",
        mock_health_check_error
    )

    # Test that the error is handled properly
    response = client.get("/api_health")

    # Ensure that the status code is 503 and that the response contains the error details
    assert response.status_code == 503
    assert response.json()["status"] == "error"
    assert "Simulated error" in response.json()["details"]
