// Script to generate 1 million customer records in MongoDB
// Run with: mongosh "mongodb+srv://admin:IdZcKnNvmWqea13k@proptuity-dev.mgzig.mongodb.net/real_transfer_test" generate_million_customers.js

const db = db.getSiblingDB('real_transfer_test');

print("🚀 Starting generation of 1,000,000 customer records...");

const TOTAL_RECORDS = 1000000;
const BATCH_SIZE = 10000; // Insert 10k at a time for optimal performance
const NUM_BATCHES = TOTAL_RECORDS / BATCH_SIZE;

// Clear existing customers collection
print("🗑️  Clearing existing customers collection...");
db.customers.drop();
print("✅ Collection cleared");

// Create indexes first for better insert performance
print("📊 Creating indexes...");
db.customers.createIndex({ customer_id: 1 }, { unique: true });
db.customers.createIndex({ email: 1 }, { unique: true });
db.customers.createIndex({ status: 1 });
db.customers.createIndex({ verified: 1 });
db.customers.createIndex({ created_at: -1 });
print("✅ Indexes created");

// Helper function to generate random data
function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomFloat(min, max) {
    return Math.random() * (max - min) + min;
}

function randomChoice(arr) {
    return arr[randomInt(0, arr.length - 1)];
}

function randomEmail(index) {
    const domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'company.com'];
    return `customer${index}@${randomChoice(domains)}`;
}

function randomPhone() {
    return `+1-${randomInt(200, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`;
}

const firstNames = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Mary', 
                    'William', 'Patricia', 'Richard', 'Jennifer', 'Charles', 'Linda', 'Joseph', 'Elizabeth', 
                    'Thomas', 'Barbara', 'Christopher', 'Susan', 'Daniel', 'Jessica', 'Matthew', 'Karen',
                    'Anthony', 'Nancy', 'Mark', 'Betty', 'Donald', 'Helen', 'Steven', 'Sandra', 'Paul', 'Donna'];

const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 
                   'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 
                   'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez',
                   'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King', 'Wright'];

const cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 
                'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 
                'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle', 'Denver', 'Boston'];

const states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'FL', 'OH', 'NC', 'WA', 'CO', 'MA'];

const preferences = {
    newsletter: [true, false],
    sms_notifications: [true, false],
    email_notifications: [true, false],
    language: ['en', 'es', 'fr', 'de', 'zh'],
    theme: ['light', 'dark', 'auto']
};

// Generate and insert customers in batches
let totalInserted = 0;
const startTime = new Date();

for (let batch = 0; batch < NUM_BATCHES; batch++) {
    const batchStartTime = new Date();
    const customers = [];
    
    for (let i = 0; i < BATCH_SIZE; i++) {
        const customerIndex = (batch * BATCH_SIZE) + i + 1;
        const isVerified = randomInt(1, 100) <= 85; // 85% verified
        const isActive = randomInt(1, 100) <= 90; // 90% active
        
        const customer = {
            customer_id: `CUST${String(customerIndex).padStart(9, '0')}`,
            first_name: randomChoice(firstNames),
            last_name: randomChoice(lastNames),
            email: randomEmail(customerIndex),
            phone: randomPhone(),
            status: isActive ? 'active' : randomChoice(['inactive', 'suspended']),
            verified: isVerified,
            address: {
                street: `${randomInt(1, 9999)} ${randomChoice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm'])} St`,
                city: randomChoice(cities),
                state: randomChoice(states),
                zip: String(randomInt(10000, 99999))
            },
            preferences: {
                newsletter: randomChoice(preferences.newsletter),
                sms_notifications: randomChoice(preferences.sms_notifications),
                email_notifications: randomChoice(preferences.email_notifications),
                language: randomChoice(preferences.language),
                theme: randomChoice(preferences.theme)
            },
            total_orders: randomInt(0, 150),
            total_spent: parseFloat(randomFloat(0, 50000).toFixed(2)),
            created_at: new Date(Date.now() - randomInt(0, 365 * 3) * 24 * 60 * 60 * 1000), // Last 3 years
            last_login: new Date(Date.now() - randomInt(0, 30) * 24 * 60 * 60 * 1000) // Last 30 days
        };
        
        customers.push(customer);
    }
    
    // Bulk insert the batch
    db.customers.insertMany(customers, { ordered: false });
    totalInserted += BATCH_SIZE;
    
    const batchEndTime = new Date();
    const batchDuration = (batchEndTime - batchStartTime) / 1000;
    const totalDuration = (batchEndTime - startTime) / 1000;
    const recordsPerSecond = Math.floor(totalInserted / totalDuration);
    const progress = ((batch + 1) / NUM_BATCHES * 100).toFixed(1);
    
    print(`✅ Batch ${batch + 1}/${NUM_BATCHES} (${progress}%): Inserted ${BATCH_SIZE} customers in ${batchDuration.toFixed(2)}s | Total: ${totalInserted.toLocaleString()} | Rate: ${recordsPerSecond.toLocaleString()} rec/s`);
}

const endTime = new Date();
const totalDuration = (endTime - startTime) / 1000;

print("\n" + "=".repeat(80));
print("🎉 GENERATION COMPLETE!");
print("=".repeat(80));
print(`📊 Total Records: ${totalInserted.toLocaleString()}`);
print(`⏱️  Total Duration: ${totalDuration.toFixed(2)} seconds`);
print(`🚀 Average Rate: ${Math.floor(totalInserted / totalDuration).toLocaleString()} records/second`);

// Verify count
const finalCount = db.customers.countDocuments();
print(`✅ Verified Count: ${finalCount.toLocaleString()} customers`);

// Show sample statistics
print("\n📈 Sample Statistics:");
print(`   Active Customers: ${db.customers.countDocuments({status: 'active'}).toLocaleString()}`);
print(`   Verified Customers: ${db.customers.countDocuments({verified: true}).toLocaleString()}`);
print(`   Average Total Spent: $${db.customers.aggregate([{$group: {_id: null, avg: {$avg: "$total_spent"}}}]).toArray()[0].avg.toFixed(2)}`);

print("\n✅ Ready for sync testing!");
