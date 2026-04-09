import { NextResponse } from 'next/server';
import bcrypt from 'bcryptjs';
import clientPromise from '@/lib/mongodb';

export async function POST(req: Request) {
  try {
    const { name, email, password } = await req.json();

    if (!email || !password) {
      return NextResponse.json(
        { message: 'Missing required parameters' },
        { status: 400 }
      );
    }

    const client = await clientPromise;
    const db = client.db();

    const normalizedEmail = email.toLowerCase();

    const existingUser = await db.collection('users').findOne({ email: normalizedEmail });
    if (existingUser) {
      return NextResponse.json(
        { message: 'User already exists' },
        { status: 422 }
      );
    }

    const hashedPassword = await bcrypt.hash(password, 12);

    const result = await db.collection('users').insertOne({
      email: normalizedEmail,
      name: name || '',
      hashedPassword,
      createdAt: new Date(),
    });

    return NextResponse.json(
      { message: 'User created', userId: result.insertedId },
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
