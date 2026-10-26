"""Add next_retry_at to task

Revision ID: 4997c0cc945e
Revises: 05454a80a25f
Create Date: 2026-09-25 22:00:23.006239

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '4997c0cc945e'
down_revision: Union[str, None] = '05454a80a25f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('task', sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=True))
    op.create_index(op.f('ix_task_next_retry_at'), 'task', ['next_retry_at'], unique=False)

def downgrade() -> None:
    op.drop_index(op.f('ix_task_next_retry_at'), table_name='task')
    op.drop_column('task', 'next_retry_at')
