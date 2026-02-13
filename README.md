# IndexQube

Financial index calculation infrastructure.

## Frontend (Next.js)

The frontend is a Next.js 15 app in the `frontend/` directory.

### Development

```bash
cd frontend
npm install
npm run dev
```

Runs at [http://localhost:3000](http://localhost:3000).

API requests to `/api/*` are proxied to the FastAPI backend (default `http://localhost:8000`). Set `NEXT_PUBLIC_API_URL` to change the API base URL.

### Production Build

```bash
cd frontend
npm run build
npm start
```

## API (FastAPI)

```bash
uvicorn api.main:app --reload
```

Runs at [http://localhost:8000](http://localhost:8000).
