"""Initial V1 metadata schema; tombstones and replica mappings are retained."""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "files",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("original_name", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("chunk_size_bytes", sa.Integer(), nullable=False),
        sa.Column("total_chunks", sa.Integer(), nullable=False),
        sa.Column("replication_factor", sa.Integer(), nullable=False),
        sa.Column("checksum_sha256", sa.String(64)),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error_code", sa.String(64)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("size_bytes >= 0 AND total_chunks >= 0 AND replication_factor >= 1"),
        sa.CheckConstraint("chunk_size_bytes > 0"),
        sa.CheckConstraint("status IN ('UPLOADING','AVAILABLE','FAILED','DELETING','DELETED')"),
    )
    op.create_table(
        "storage_nodes",
        sa.Column("node_id", sa.String(64), primary_key=True),
        sa.Column("host", sa.String(255), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("failure_domain", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(255)),
        sa.Column("capacity_bytes", sa.BigInteger()),
        sa.Column("available_bytes", sa.BigInteger()),
        sa.Column("used_bytes", sa.BigInteger()),
        sa.CheckConstraint("port BETWEEN 1 AND 65535"),
        sa.CheckConstraint("status IN ('ACTIVE','SUSPECTED','DOWN')"),
        sa.CheckConstraint("capacity_bytes >= 0 AND available_bytes >= 0 AND used_bytes >= 0"),
    )
    op.create_table(
        "chunks",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("file_id", sa.Uuid(), sa.ForeignKey("files.id"), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("checksum_sha256", sa.String(64), nullable=False),
        sa.UniqueConstraint("file_id", "chunk_index"),
        sa.CheckConstraint("chunk_index >= 0 AND size_bytes > 0"),
    )
    op.create_index("ix_chunks_file_id", "chunks", ["file_id"])
    op.create_table(
        "chunk_replicas",
        sa.Column("chunk_id", sa.Uuid(), sa.ForeignKey("chunks.id"), primary_key=True),
        sa.Column(
            "node_id", sa.String(64), sa.ForeignKey("storage_nodes.node_id"), primary_key=True
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("cleanup_pending", sa.Boolean(), nullable=False),
        sa.Column("last_verified_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(255)),
        sa.CheckConstraint("status IN ('PENDING','VERIFIED','MISSING','CORRUPTED','DELETED')"),
    )


def downgrade():
    op.drop_table("chunk_replicas")
    op.drop_index("ix_chunks_file_id", table_name="chunks")
    op.drop_table("chunks")
    op.drop_table("storage_nodes")
    op.drop_table("files")
