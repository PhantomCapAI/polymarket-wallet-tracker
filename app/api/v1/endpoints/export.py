from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.connection import get_db
from app.utils.excel_export import excel_export_service

router = APIRouter()

@router.get("/export/excel")
async def export_excel(db: AsyncSession = Depends(get_db)):
    """Export all data to Excel file"""
    try:
        excel_data = await excel_export_service.export_all_tables(db)
        
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=phantom_capital_data.xlsx"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
