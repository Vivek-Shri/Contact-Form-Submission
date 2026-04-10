import { NextResponse } from 'next/server';
import bcrypt from 'bcryptjs';
import pool from '@/lib/db';

export async function POST(req: Request) {
  try {
    const { name, email, password } = await req.json();

    if (!email || !password) {
      return NextResponse.json(
        { message: 'Missing required parameters' },
        { status: 400 }
      );
    }

    const normalizedEmail = email.toLowerCase();

    const existingRes = await pool.query('SELECT * FROM users WHERE email = $1', [normalizedEmail]);
    if (existingRes.rows.length > 0) {
      return NextResponse.json(
        { message: 'User already exists' },
        { status: 422 }
      );
    }

    const hashedPassword = await bcrypt.hash(password, 12);

    // Auto-promote the first user to admin
    const countRes = await pool.query('SELECT COUNT(*) FROM users');
    const isFirstUser = parseInt(countRes.rows[0].count, 10) === 0;

    const insertRes = await pool.query(
      'INSERT INTO users (email, name, hashed_password, created_at, is_admin) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [normalizedEmail, name || '', hashedPassword, new Date().toISOString(), isFirstUser]
    );

    return NextResponse.json(
      { message: 'User created', userId: insertRes.rows[0].id },
      { status: 201 }
    );
  } catch (error) {
    console.error('Registration error:', error);
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    );
  }
}
